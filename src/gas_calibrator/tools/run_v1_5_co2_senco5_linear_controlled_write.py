"""Run controlled V1.5 CO2 SENCO5 affine trim writes.

SENCO5 is the final CO2 concentration affine layer:

    corrected_CO2 = raw_CO2 * C1 + C0

This tool writes only reviewed SENCO5 C0/C1 rows. It never writes
SENCO1/SENCO3/SENCO6, never changes analyzer IDs, and never controls PACE,
valves, water routes, gas routes, or humidity generation.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from ..config import load_config
from ..devices import GasAnalyzer
from ..senco_format import format_senco_value
from ..validation.reporting import ValidationMetadata, write_validation_report
from . import run_v1_5_co2_senco1_controlled_write as base


CONFIRMATION_TEXT = "WRITE_SENCO5_LINEAR_V1_5_CO2_TRIM"


def _read_csv(path: Path) -> List[Dict[str, str]]:
    with Path(path).open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _finite(value: Any, label: str) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{label} must be finite")
    return number


def _format_decimal(value: Any, label: str, *, decimals: int) -> str:
    number = _finite(value, label)
    places = max(0, int(decimals))
    if places > 3:
        raise ValueError(f"{label} payload supports at most 3 decimal places")
    step = 10 ** -places
    if abs(number) < 0.5 * step:
        number = 0.0
    return f"{number:.{places}f}"


def _format_c0(value: Any, *, decimals: int = 3) -> str:
    return _format_decimal(value, "SENCO5 C0", decimals=decimals)


def _format_c1(value: Any, *, decimals: int = 3) -> str:
    return _format_decimal(value, "SENCO5 C1", decimals=decimals)


def _format_payload_value(
    value: Any,
    *,
    value_format: str,
    is_c0: bool,
    c0_decimals: int,
    c1_decimals: int,
) -> str:
    if str(value_format or "").strip().lower() == "senco":
        return format_senco_value(value)
    return _format_c0(value, decimals=c0_decimals) if is_c0 else _format_c1(value, decimals=c1_decimals)


def _payload_value_strings(
    c0: Any,
    c1: Any,
    *,
    value_format: str,
    c0_decimals: int,
    c1_decimals: int,
) -> List[str]:
    return [
        _format_payload_value(c0, value_format=value_format, is_c0=True, c0_decimals=c0_decimals, c1_decimals=c1_decimals),
        _format_payload_value(c1, value_format=value_format, is_c0=False, c0_decimals=c0_decimals, c1_decimals=c1_decimals),
    ]


def _candidate_rows(path: Path, selected_device_ids: Sequence[str]) -> List[Dict[str, Any]]:
    selected = {base._device_id(item) for item in selected_device_ids if str(item or "").strip()}
    rows: List[Dict[str, Any]] = []
    for row in _read_csv(path):
        device_id = base._device_id(row.get("device_id") or row.get("analyzer_device_id"))
        if selected and device_id not in selected:
            continue
        if str(row.get("senco_group") or "").strip().upper() != "SENCO5":
            continue
        status = str(row.get("candidate_status") or "").strip()
        if status != "review_ready":
            continue
        c0 = _finite(row.get("C0"), "SENCO5 C0")
        c1 = _finite(row.get("C1"), "SENCO5 C1")
        if abs(c1) < 1e-12:
            continue
        item = dict(row)
        item["device_id"] = device_id
        item["_target_values"] = [c0, c1]
        rows.append(item)
    return rows


def _read_senco5(ga: GasAnalyzer, *, attempts: int, retry_delay_s: float) -> List[float]:
    return base._read_group_values_with_retry(
        ga,
        5,
        min_count=2,
        attempts=max(1, int(attempts)),
        retry_delay_s=max(0.0, float(retry_delay_s)),
    )


def _matches_senco5(
    expected: Sequence[float],
    actual: Sequence[float],
    *,
    c0_atol: float,
    c1_atol: float,
    c0_decimals: int,
    c1_decimals: int,
) -> bool:
    if len(actual) < 2 or len(expected) < 2:
        return False
    expected_c0 = float(_format_c0(expected[0], decimals=c0_decimals))
    expected_c1 = float(_format_c1(expected[1], decimals=c1_decimals))
    return (
        abs(float(actual[0]) - expected_c0) <= float(c0_atol)
        and abs(float(actual[1]) - expected_c1) <= float(c1_atol)
    )


def _write_senco5(
    ga: GasAnalyzer,
    *,
    device_id: str,
    c0: float,
    c1: float,
    target_mode: str,
    payload_width: int,
    value_format: str,
    c0_decimals: int,
    c1_decimals: int,
    ack_timeout_s: float,
) -> tuple[bool, str, List[str]]:
    prepare = getattr(ga, "_prepare_coefficient_io", None)
    if callable(prepare):
        prepare()
    target = device_id if str(target_mode or "").strip().lower() == "device_id" else getattr(ga, "COMMAND_TARGET_ID", "FFF")
    values = [
        _format_payload_value(c0, value_format=value_format, is_c0=True, c0_decimals=c0_decimals, c1_decimals=c1_decimals),
        _format_payload_value(c1, value_format=value_format, is_c0=False, c0_decimals=c0_decimals, c1_decimals=c1_decimals),
    ]
    if int(payload_width) == 6:
        zero = _format_payload_value(0.0, value_format=value_format, is_c0=False, c0_decimals=c0_decimals, c1_decimals=c1_decimals)
        values.extend([zero, zero, zero, zero])
    payload = f"SENCO5,YGAS,{target}," + ",".join(values)
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
        lines: List[str] = []
        for line in raw_lines:
            lines.extend(GasAnalyzer._split_stream_lines(line))
        return any(GasAnalyzer._is_success_ack(line) for line in lines), payload, lines

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
    parser = argparse.ArgumentParser(description="Controlled V1.5 CO2 SENCO5 affine-trim writer.")
    parser.add_argument("--config", required=True, help="V1.5 hardware config JSON.")
    parser.add_argument("--candidate-coefficients-csv", required=True, help="Reviewed SENCO5 candidate coefficient CSV.")
    parser.add_argument("--output-dir", required=True, help="Output directory for write evidence.")
    parser.add_argument("--device-id", action="append", default=[], help="Analyzer MODE2 device ID to write.")
    parser.add_argument("--write-all-ready", action="store_true", help="Write every review_ready candidate in the CSV.")
    parser.add_argument("--enable-senco5-write", action="store_true", help="Unlock the real SENCO5 write path.")
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
    parser.add_argument("--coefficient-read-delay-s", type=float, default=0.5)
    parser.add_argument("--coefficient-read-retries", type=int, default=4)
    parser.add_argument("--c0-compare-atol", type=float, default=0.05)
    parser.add_argument("--c1-compare-atol", type=float, default=0.00005)
    parser.add_argument("--senco5-target-mode", choices=("broadcast", "device_id"), default="broadcast")
    parser.add_argument(
        "--senco5-value-format",
        choices=("senco", "decimal"),
        default="decimal",
        help="SENCO5 is the final affine concentration layer and defaults to decimal C0/C1 payload.",
    )
    parser.add_argument(
        "--senco5-payload-width",
        type=int,
        choices=(2, 6),
        default=2,
        help="SENCO5 write payload coefficient count. The V1.5 final affine layer contract is C0,C1.",
    )
    parser.add_argument("--senco5-c0-decimals", type=int, default=3, help="Decimal places for SENCO5 C0 payload, max 3.")
    parser.add_argument(
        "--senco5-c1-decimals",
        type=int,
        default=3,
        help="Decimal places for SENCO5 C1 payload, max 3.",
    )
    parser.add_argument("--senco5-ack-timeout-s", type=float, default=2.5)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    if not bool(args.enable_senco5_write) or str(args.operator_confirmation).strip() != CONFIRMATION_TEXT:
        print("SENCO5 linear write locked: explicit unlock and operator confirmation are required.", file=sys.stderr)
        return 2
    if not str(args.reviewer).strip() or not str(args.approver).strip():
        print("SENCO5 linear write locked: reviewer and approver are required.", file=sys.stderr)
        return 2
    if str(args.reviewer).strip() == str(args.approver).strip():
        print("SENCO5 linear write locked: reviewer and approver must differ.", file=sys.stderr)
        return 2

    candidate_path = Path(args.candidate_coefficients_csv).resolve()
    candidates = _candidate_rows(candidate_path, args.device_id)
    if not bool(args.write_all_ready) and not args.device_id:
        print("SENCO5 linear write locked: pass --device-id or --write-all-ready.", file=sys.stderr)
        return 2
    if not candidates:
        print("No review_ready SENCO5 candidates selected.", file=sys.stderr)
        return 2

    cfg_path = Path(args.config).resolve()
    cfg = load_config(cfg_path)
    analyzer_map = base._build_analyzer_map(cfg)
    rows: List[Dict[str, Any]] = []
    old_snapshot: Dict[str, Any] = {}
    overall_ok = True
    start_ts = datetime.now().isoformat(timespec="seconds")

    for idx, candidate in enumerate(candidates):
        device_id = base._device_id(candidate.get("device_id"))
        analyzer_cfg = analyzer_map.get(device_id)
        if analyzer_cfg is None:
            rows.append({"device_id": device_id, "status": "skipped", "reason": "device_id_not_in_config"})
            overall_ok = False
            if not args.continue_on_failure:
                break
            continue

        target = [float(value) for value in candidate["_target_values"]]
        payload_values = _payload_value_strings(
            target[0],
            target[1],
            value_format=str(args.senco5_value_format),
            c0_decimals=int(args.senco5_c0_decimals),
            c1_decimals=int(args.senco5_c1_decimals),
        )
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
        ack_lines: List[str] = []
        status = "failed"
        reason = ""
        write_applied = False
        try:
            base._log(f"CO2 SENCO5 linear controlled write begin: device_id={device_id} port={analyzer_cfg.get('port')}")
            ga.open()
            base._configure_coefficient_io(ga, args)
            base._sleep_gap(float(args.pre_device_cooldown_s))
            identity_before = base._read_identity_snapshot(
                ga,
                prefer_stream=bool(analyzer_cfg.get("active_send", True)),
                timeout_s=float(args.identity_timeout_s),
            )
            if base._device_id(identity_before.get("id")) != device_id:
                raise RuntimeError(f"identity_mismatch expected={device_id} observed={identity_before.get('id') or '<missing>'}")

            old_live = _read_senco5(
                ga,
                attempts=max(1, int(args.readback_attempts)),
                retry_delay_s=float(args.readback_retry_delay_s),
            )
            if not ga.set_mode_with_ack(2, require_ack=True):
                raise RuntimeError("MODE=2 not acknowledged before SENCO5 write")
            base._sleep_gap(float(args.readback_retry_delay_s))
            acked, payload, ack_lines = _write_senco5(
                ga,
                device_id=device_id,
                c0=target[0],
                c1=target[1],
                target_mode=str(args.senco5_target_mode),
                payload_width=int(args.senco5_payload_width),
                value_format=str(args.senco5_value_format),
                c0_decimals=int(args.senco5_c0_decimals),
                c1_decimals=int(args.senco5_c1_decimals),
                ack_timeout_s=float(args.senco5_ack_timeout_s),
            )
            write_applied = True
            base._sleep_gap(max(0.0, float(args.post_write_settle_s)))
            final_live = _read_senco5(
                ga,
                attempts=max(1, int(args.readback_attempts)),
                retry_delay_s=float(args.readback_retry_delay_s),
            )
            if not _matches_senco5(
                target,
                final_live,
                c0_atol=float(args.c0_compare_atol),
                c1_atol=float(args.c1_compare_atol),
                c0_decimals=int(args.senco5_c0_decimals),
                c1_decimals=int(args.senco5_c1_decimals),
            ):
                raise RuntimeError(f"SENCO5_READBACK_MISMATCH target={payload_values} final={final_live}")
            if not acked:
                reason = f"SENCO5_WRITE_ACK_MISSING_BUT_READBACK_MATCHED response={ack_lines}"

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
                raise RuntimeError(f"post_write_identity_mismatch expected={device_id} observed={identity_after.get('id') or '<missing>'}")
            status = "written_readback_verified" if acked else "written_readback_verified_ack_missing"
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
            "GETCO5_before_live": old_live,
            "GETCO5_after_live": final_live,
            "target_senco5": target,
        }
        rows.append(
            {
                "device_id": device_id,
                "analyzer_prefix": analyzer_cfg.get("name", ""),
                "port": analyzer_cfg.get("port", ""),
                "old_senco5": json.dumps(old_live, ensure_ascii=False),
                "target_senco5": json.dumps(target, ensure_ascii=False),
                "payload_values": json.dumps(payload_values, ensure_ascii=False),
                "payload": payload,
                "final_senco5": json.dumps(final_live, ensure_ascii=False),
                "write_ack_response_json": json.dumps(ack_lines, ensure_ascii=False),
                "status": status,
                "reason": reason,
                "write_applied": write_applied and status.startswith("written_readback_verified"),
                "identity_before": identity_before.get("id", ""),
                "identity_after": identity_after.get("id", ""),
                "runtime_restore_status": restore.get("status", ""),
                "active_freq_restore_status": restore.get("active_freq_restore_status", ""),
                "writes_senco1": False,
                "writes_senco3": False,
                "writes_senco5": status.startswith("written_readback_verified"),
                "writes_senco6": False,
                "writes_device_id": False,
                "clears_senco": False,
                "controls_water_or_gas_routes": False,
                "controls_pace": False,
                "reviewer": str(args.reviewer),
                "approver": str(args.approver),
            }
        )
        if not status.startswith("written_readback_verified"):
            overall_ok = False
            if not args.continue_on_failure:
                break
        if idx + 1 < len(candidates):
            base._sleep_gap(float(args.inter_device_delay_s))

    end_ts = datetime.now().isoformat(timespec="seconds")
    conclusion = {
        "overall_status": "success" if overall_ok else "failed",
        "started_at": start_ts,
        "finished_at": end_ts,
        "target_count": len(candidates),
        "processed_count": len(rows),
        "success_count": sum(1 for row in rows if str(row.get("status") or "").startswith("written_readback_verified")),
        "controls_water_or_gas_routes": False,
        "writes_device_id": False,
        "writes_senco5": True,
        "writes_senco6": False,
        "clears_senco": False,
    }

    base._write_csv(output_dir / "senco5_linear_write_events.csv", rows)
    base._write_csv(output_dir / "conclusion.csv", [conclusion])
    (output_dir / "old_senco5_snapshot.json").write_text(
        json.dumps(old_snapshot, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    (output_dir / "senco5_linear_write_meta.json").write_text(
        json.dumps(
            {
                "created_at": end_ts,
                "tool": "run_v1_5_co2_senco5_linear_controlled_write",
                "candidate_coefficients_csv": str(candidate_path),
                "confirmation_text": CONFIRMATION_TEXT,
                "reviewer": args.reviewer,
                "approver": args.approver,
                "writes_senco1": False,
                "writes_senco3": False,
                "writes_senco5": True,
                "writes_senco6": False,
                "writes_device_id": False,
                "clears_senco": False,
                "controls_water_or_gas_routes": False,
                "controls_pace": False,
                "c0_compare_atol": float(args.c0_compare_atol),
                "c1_compare_atol": float(args.c1_compare_atol),
                "senco5_payload_width": int(args.senco5_payload_width),
                "senco5_value_format": str(args.senco5_value_format),
                "senco5_c0_decimals": int(args.senco5_c0_decimals),
                "senco5_c1_decimals": int(args.senco5_c1_decimals),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    metadata = ValidationMetadata(
        tool_name="run_v1_5_co2_senco5_linear_controlled_write",
        created_at=end_ts,
        analyzers=[str(row.get("device_id") or "") for row in rows],
        input_paths=[str(cfg_path), str(candidate_path)],
        output_dir=str(output_dir),
        config_path=str(cfg_path),
        config_summary={
            "status": str(conclusion["overall_status"]),
            "device_ids": [base._device_id(item) for item in args.device_id],
            "reviewer": str(args.reviewer),
            "approver": str(args.approver),
            "restore_active_freq": bool(args.restore_active_freq),
            "c0_compare_atol": float(args.c0_compare_atol),
            "c1_compare_atol": float(args.c1_compare_atol),
        },
        notes=[
            "Controlled real-device CO2 SENCO5 affine trim write.",
            "SENCO5 applies corrected concentration = concentration*C1 + C0.",
            (
                "Payload format is explicit and recorded; the physical layer remains "
                "corrected concentration = concentration*C1 + C0."
            ),
            "C0 and C1 decimal precision are explicit operator-visible parameters; C1 should keep enough digits to preserve multiplier corrections.",
            "No PACE, valve, water route, gas route, humidity generator, device-ID writes, SENCO1/SENCO3 writes, or SENCO6 writes are performed.",
        ],
    )
    write_validation_report(
        output_dir,
        tables={"senco5_linear_write_events": rows, "conclusion": [conclusion]},
        metadata=metadata,
        prefix="co2_senco5_linear_controlled_write",
    )
    print(json.dumps(conclusion, ensure_ascii=False, indent=2), flush=True)
    return 0 if overall_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
