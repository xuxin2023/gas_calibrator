"""Run controlled V1.5 SENCO9 clear/recovery writes.

This tool is for a narrow pressure-channel recovery case: an analyzer pressure
field is clamped or fixed, so multi-pressure SENCO9 fitting has no observable
pressure response. It clears only SENCO9 on one analyzer port at a time, verifies
GETCO9 readback, and never controls PACE, valves, water routes, gas routes,
humidity generation, CO2/H2O coefficients, or device IDs.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from ..config import load_config
from ..devices import GasAnalyzer
from ..validation.reporting import ValidationMetadata, write_validation_report
from . import run_v1_5_pressure_senco9_controlled_write as base
from .v1_5_serial_safety import require_fragile_serial_timing


CONFIRMATION_TEXT = "CLEAR_SENCO9_V1_5_PRESSURE_RECOVERY_ONLY"
TARGET_SENCO9 = (0.0, 1.0, 0.0, 0.0)


def _is_neutral(values: Sequence[float], *, atol: float = 0.05) -> bool:
    if len(values) < 2:
        return False
    padded = [float(item) for item in list(values)[:4]]
    while len(padded) < 4:
        padded.append(0.0)
    return (
        abs(padded[0]) <= float(atol)
        and abs(padded[1] - 1.0) <= float(atol)
        and abs(padded[2]) <= float(atol)
        and abs(padded[3]) <= float(atol)
    )


def _clear_senco9(
    ga: GasAnalyzer,
    *,
    device_id: str,
    target_mode: str,
    ack_timeout_s: float,
) -> tuple[bool, str, List[str]]:
    prepare = getattr(ga, "_prepare_coefficient_io", None)
    if callable(prepare):
        prepare()
    target = device_id if str(target_mode or "").strip().lower() == "device_id" else getattr(ga, "COMMAND_TARGET_ID", "FFF")
    payload = f"CLEARSENCO9,YGAS,{target}"
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
        return acked, payload, response_lines

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
    return acked, payload, []


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Controlled V1.5 SENCO9 pressure recovery clear writer.")
    parser.add_argument("--config", required=True, help="V1.5 hardware config JSON.")
    parser.add_argument("--output-dir", required=True, help="Output directory for clear evidence.")
    parser.add_argument("--device-id", action="append", default=[], help="Analyzer MODE2 device ID to consider.")
    parser.add_argument("--clear-all-nonneutral", action="store_true", help="Clear every selected non-neutral SENCO9.")
    parser.add_argument("--enable-senco9-clear", action="store_true", help="Unlock the real CLEARSENCO9 path.")
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
        "--senco9-target-mode",
        choices=("broadcast", "device_id"),
        default="broadcast",
        help="Use CLEARSENCO9,YGAS,FFF or CLEARSENCO9,YGAS,<device_id> for recovery.",
    )
    parser.add_argument("--senco9-ack-timeout-s", type=float, default=2.5)
    return parser.parse_args(list(argv) if argv is not None else None)


def _read_senco9(ga: GasAnalyzer, *, attempts: int, retry_delay_s: float) -> List[float]:
    last_error = ""
    for idx in range(max(1, int(attempts))):
        try:
            return base._read_getco9_values(ga)
        except Exception as exc:
            last_error = str(exc)
            if idx + 1 < max(1, int(attempts)):
                base._sleep_gap(max(0.0, float(retry_delay_s)))
    raise RuntimeError(last_error or "GETCO9 read failed")


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        require_fragile_serial_timing(args, tool_name="run_v1_5_pressure_senco9_clear_controlled_write")
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    if not bool(args.enable_senco9_clear) or str(args.operator_confirmation).strip() != CONFIRMATION_TEXT:
        print("SENCO9 clear locked: explicit unlock and operator confirmation are required.", file=sys.stderr)
        return 2
    if not str(args.reviewer).strip() or not str(args.approver).strip():
        print("SENCO9 clear locked: reviewer and approver are required.", file=sys.stderr)
        return 2
    if str(args.reviewer).strip() == str(args.approver).strip():
        print("SENCO9 clear locked: reviewer and approver must differ.", file=sys.stderr)
        return 2

    cfg = load_config(args.config)
    analyzer_map = base._build_analyzer_map(cfg)
    selected = [base._device_id(item) for item in args.device_id if str(item or "").strip()]
    target_ids = selected or sorted(analyzer_map)
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
        old_live: List[float] = []
        final_live: List[float] = []
        payload = ""
        write_ack_lines: List[str] = []
        status = "failed"
        reason = ""
        write_applied = False

        try:
            base._log(f"Pressure SENCO9 recovery clear begin: device_id={device_id} port={analyzer_cfg.get('port')}")
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

            old_live = _read_senco9(
                ga,
                attempts=max(1, int(args.readback_attempts)),
                retry_delay_s=float(args.readback_retry_delay_s),
            )
            final_live = list(old_live)
            if _is_neutral(old_live, atol=float(args.compare_atol)):
                status = "already_neutral"
            else:
                if not args.clear_all_nonneutral and not selected:
                    status = "blocked"
                    reason = "clear_all_nonneutral_or_explicit_device_required"
                else:
                    if not ga.set_mode_with_ack(2, require_ack=True):
                        raise RuntimeError("MODE=2 not acknowledged before SENCO9 clear")
                    base._sleep_gap(float(args.readback_retry_delay_s))
                    acked, payload, write_ack_lines = _clear_senco9(
                        ga,
                        device_id=device_id,
                        target_mode=str(args.senco9_target_mode),
                        ack_timeout_s=float(args.senco9_ack_timeout_s),
                    )
                    write_applied = True
                    if not acked:
                        raise RuntimeError(f"SENCO9_CLEAR_ACK_FAILED response={write_ack_lines}")
                    base._sleep_gap(max(0.0, float(args.post_write_settle_s)))
                    final_live = _read_senco9(
                        ga,
                        attempts=max(1, int(args.readback_attempts)),
                        retry_delay_s=float(args.readback_retry_delay_s),
                    )
                    if not _is_neutral(final_live, atol=float(args.compare_atol)):
                        raise RuntimeError(f"SENCO9_READBACK_MISMATCH final={final_live}")
                    status = "written_readback_verified"

            restore = base._restore_analyzer_runtime(
                ga,
                analyzer_cfg,
                command_gap_s=float(args.restore_command_gap_s),
                restore_active_freq=bool(args.restore_active_freq),
            )
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
        except Exception as exc:
            reason = str(exc)
            overall_ok = False
            try:
                restore = base._restore_analyzer_runtime(
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
            "analyzer_prefix": analyzer_cfg.get("name", ""),
            "port": analyzer_cfg.get("port", ""),
            "GETCO9_before_live": old_live,
            "GETCO9_after_live": final_live,
        }
        if status not in {"written_readback_verified", "already_neutral"}:
            overall_ok = False
        rows.append(
            {
                "device_id": device_id,
                "analyzer_prefix": analyzer_cfg.get("name", ""),
                "port": analyzer_cfg.get("port", ""),
                "old_senco9": json.dumps(old_live, ensure_ascii=False),
                "target_senco9": json.dumps(list(TARGET_SENCO9), ensure_ascii=False),
                "final_senco9": json.dumps(final_live, ensure_ascii=False),
                "payload": payload,
                "payload_target_mode": str(args.senco9_target_mode),
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
                "writes_senco9": bool(write_applied),
                "writes_device_id": False,
                "clears_senco": bool(write_applied),
                "clears_senco9": bool(write_applied),
                "controls_water_or_gas_routes": False,
                "controls_pace": False,
            }
        )
        if idx + 1 < len(target_ids):
            base._sleep_gap(float(args.inter_device_delay_s))
        if status not in {"written_readback_verified", "already_neutral"} and not args.continue_on_failure:
            break

    conclusion = {
        "overall_status": "success" if overall_ok else "failed",
        "target_count": len(rows),
        "success_count": sum(1 for row in rows if row.get("status") == "written_readback_verified"),
        "already_neutral_count": sum(1 for row in rows if row.get("status") == "already_neutral"),
        "failed_count": sum(1 for row in rows if row.get("status") not in {"written_readback_verified", "already_neutral"}),
    }

    base._write_csv(output_dir / "senco9_clear_write_events.csv", rows)
    base._write_csv(output_dir / "conclusion.csv", [conclusion])
    (output_dir / "old_senco9_snapshot.json").write_text(
        json.dumps(old_snapshot, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    (output_dir / "senco9_clear_write_meta.json").write_text(
        json.dumps(
            {
                "created_at": datetime.now().isoformat(timespec="seconds"),
                "tool": "run_v1_5_pressure_senco9_clear_controlled_write",
                "confirmation_text": CONFIRMATION_TEXT,
                "reviewer": args.reviewer,
                "approver": args.approver,
                "target_senco9": list(TARGET_SENCO9),
                "senco9_target_mode": str(args.senco9_target_mode),
                "senco9_ack_timeout_s": float(args.senco9_ack_timeout_s),
                "writes_senco9": True,
                "writes_device_id": False,
                "clears_senco": True,
                "clears_senco9": True,
                "controls_water_or_gas_routes": False,
                "controls_pace": False,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    metadata = ValidationMetadata(
        tool_name="run_v1_5_pressure_senco9_clear_controlled_write",
        analyzers=[str(row.get("device_id") or "") for row in rows],
        config_path=str(Path(args.config).resolve()),
        output_dir=str(output_dir),
        config_summary={
            "suite_id": "v1_5_pressure_senco9_clear_recovery",
            "status": str(conclusion["overall_status"]),
            "target_senco9": list(TARGET_SENCO9),
            "senco9_target_mode": str(args.senco9_target_mode),
            "senco9_ack_timeout_s": float(args.senco9_ack_timeout_s),
            "restore_active_freq": bool(args.restore_active_freq),
            "compare_atol": float(args.compare_atol),
        },
        notes=[
            "Controlled real-device pressure SENCO9 clear/recovery write.",
            "This recovery is only for restoring pressure response before no-write multi-pressure fitting.",
            "Neutralization uses CLEARSENCO9 and must be verified by GETCO9 readback.",
            "No PACE, valve, water route, gas route, humidity generator, device-ID writes, or CO2/H2O coefficient writes are performed.",
        ],
    )
    write_validation_report(
        output_dir,
        tables={
            "senco9_clear_write_events": rows,
            "conclusion": [conclusion],
        },
        metadata=metadata,
        prefix="pressure_senco9_clear_controlled_write",
    )
    print(json.dumps(conclusion, ensure_ascii=False, indent=2), flush=True)
    return 0 if overall_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
