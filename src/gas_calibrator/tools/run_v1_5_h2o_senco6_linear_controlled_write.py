"""Run controlled V1.5 H2O SENCO6 affine trim writes.

SENCO6 is the final H2O concentration affine layer:

    corrected_H2O = raw_H2O * C1 + C0

This tool writes only reviewed SENCO6 C0/C1 rows. It never writes
SENCO1/SENCO2/SENCO3/SENCO4/SENCO5, never changes analyzer IDs, and never
controls PACE, valves, gas routes, water routes, or humidity generation.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

from ..config import load_config
from ..devices import GasAnalyzer
from ..validation.reporting import ValidationMetadata, write_validation_report
from ..validation.v1_5_final_senco_prewrite_gate import validate_final_senco_prewrite_gate
from . import run_v1_5_co2_senco1_controlled_write as base
from .v1_5_serial_safety import require_fragile_serial_timing


CONFIRMATION_TEXT = "WRITE_SENCO6_LINEAR_V1_5_H2O_TRIM"


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
    return _format_decimal(value, "SENCO6 C0", decimals=decimals)


def _format_c1(value: Any, *, decimals: int = 3) -> str:
    return _format_decimal(value, "SENCO6 C1", decimals=decimals)


def _payload_value_strings(c0: Any, c1: Any, *, c0_decimals: int, c1_decimals: int) -> List[str]:
    return [
        _format_c0(c0, decimals=c0_decimals),
        _format_c1(c1, decimals=c1_decimals),
    ]


def _input_source_contract_allows_write(row: Dict[str, Any]) -> bool:
    source = str(row.get("input_source_contract") or "").strip().lower()
    if not source:
        return False
    model_only_markers = (
        "model_pred",
        "model_prediction",
        "senco24_model",
        "main_model_pred",
        "not_current_firmware",
    )
    if any(marker in source for marker in model_only_markers):
        return False
    firmware_markers = (
        "firmware",
        "postwrite",
        "post_write",
        "reported_output",
        "analyzer_reported",
        "live_output",
    )
    return any(marker in source for marker in firmware_markers)


def _candidate_rows(path: Path, selected_device_ids: Sequence[str]) -> List[Dict[str, Any]]:
    selected = {base._device_id(item) for item in selected_device_ids if str(item or "").strip()}
    rows: List[Dict[str, Any]] = []
    for row in _read_csv(path):
        device_id = base._device_id(row.get("device_id") or row.get("analyzer_device_id"))
        if selected and device_id not in selected:
            continue
        if str(row.get("senco_group") or "").strip().upper() != "SENCO6":
            continue
        if str(row.get("candidate_status") or "").strip() != "review_ready":
            continue
        if not _input_source_contract_allows_write(row):
            continue
        c0 = _finite(row.get("C0"), "SENCO6 C0")
        c1 = _finite(row.get("C1"), "SENCO6 C1")
        if abs(c1) < 1e-12:
            continue
        item = dict(row)
        item["device_id"] = device_id
        item["_target_values"] = [c0, c1]
        rows.append(item)
    return rows


def _read_senco6(ga: GasAnalyzer, *, attempts: int, retry_delay_s: float) -> List[float]:
    return base._read_group_values_with_retry(
        ga,
        6,
        min_count=2,
        attempts=max(1, int(attempts)),
        retry_delay_s=max(0.0, float(retry_delay_s)),
    )


def _matches_senco6(
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


def _write_senco6(
    ga: GasAnalyzer,
    *,
    device_id: str,
    c0: float,
    c1: float,
    target_mode: str,
    c0_decimals: int,
    c1_decimals: int,
    ack_timeout_s: float,
) -> tuple[bool, str, List[str]]:
    prepare = getattr(ga, "_prepare_coefficient_io", None)
    if callable(prepare):
        prepare()
    target = device_id if str(target_mode or "").strip().lower() == "device_id" else getattr(ga, "COMMAND_TARGET_ID", "FFF")
    payload_values = _payload_value_strings(c0, c1, c0_decimals=c0_decimals, c1_decimals=c1_decimals)
    payload = f"SENCO6,YGAS,{target}," + ",".join(payload_values)
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
    parser = argparse.ArgumentParser(description="Controlled V1.5 H2O SENCO6 affine-trim writer.")
    parser.add_argument("--config", required=True, help="V1.5 hardware config JSON.")
    parser.add_argument("--candidate-coefficients-csv", required=True, help="Reviewed SENCO6 candidate coefficient CSV.")
    parser.add_argument(
        "--main-senco-precheck-dir",
        default="",
        help="Required #78 final SENCO precheck directory with passing H2O fit-input traceability.",
    )
    parser.add_argument("--output-dir", required=True, help="Output directory for write evidence.")
    parser.add_argument("--device-id", action="append", default=[], help="Analyzer MODE2 device ID to write.")
    parser.add_argument("--write-all-ready", action="store_true", help="Write every review_ready candidate in the CSV.")
    parser.add_argument("--enable-senco6-write", action="store_true", help="Unlock the real SENCO6 write path.")
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
    parser.add_argument("--c0-compare-atol", type=float, default=0.05)
    parser.add_argument("--c1-compare-atol", type=float, default=0.00005)
    parser.add_argument("--senco6-target-mode", choices=("broadcast", "device_id"), default="broadcast")
    parser.add_argument("--senco6-c0-decimals", type=int, default=3, help="Decimal places for SENCO6 C0 payload, max 3.")
    parser.add_argument(
        "--senco6-c1-decimals",
        type=int,
        default=3,
        help="Decimal places for SENCO6 C1 payload, max 3.",
    )
    parser.add_argument("--senco6-ack-timeout-s", type=float, default=2.5)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        require_fragile_serial_timing(args, tool_name="run_v1_5_h2o_senco6_linear_controlled_write")
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    if not bool(args.enable_senco6_write) or str(args.operator_confirmation).strip() != CONFIRMATION_TEXT:
        print("SENCO6 linear write locked: explicit unlock and operator confirmation are required.", file=sys.stderr)
        return 2
    if not str(args.reviewer).strip() or not str(args.approver).strip():
        print("SENCO6 linear write locked: reviewer and approver are required.", file=sys.stderr)
        return 2
    if str(args.reviewer).strip() == str(args.approver).strip():
        print("SENCO6 linear write locked: reviewer and approver must differ.", file=sys.stderr)
        return 2

    candidate_path = Path(args.candidate_coefficients_csv).resolve()
    candidates = _candidate_rows(candidate_path, args.device_id)
    if not bool(args.write_all_ready) and not args.device_id:
        print("SENCO6 linear write locked: pass --device-id or --write-all-ready.", file=sys.stderr)
        return 2
    if not candidates:
        print("No review_ready SENCO6 candidates selected.", file=sys.stderr)
        return 2
    prewrite_ok, prewrite_reasons, prewrite_detail = validate_final_senco_prewrite_gate(
        args.main_senco_precheck_dir,
        component="h2o",
        device_ids=[base._device_id(row.get("device_id")) for row in candidates],
    )
    if not prewrite_ok:
        print(
            "SENCO6 linear write locked: final fit-input prewrite gate failed "
            f"({';'.join(prewrite_reasons)}).",
            file=sys.stderr,
        )
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
            c0_decimals=int(args.senco6_c0_decimals),
            c1_decimals=int(args.senco6_c1_decimals),
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
            base._log(f"H2O SENCO6 linear controlled write begin: device_id={device_id} port={analyzer_cfg.get('port')}")
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

            old_live = _read_senco6(
                ga,
                attempts=max(1, int(args.readback_attempts)),
                retry_delay_s=float(args.readback_retry_delay_s),
            )
            if not ga.set_mode_with_ack(2, require_ack=True):
                raise RuntimeError("MODE=2 not acknowledged before SENCO6 write")
            base._sleep_gap(float(args.readback_retry_delay_s))
            acked, payload, ack_lines = _write_senco6(
                ga,
                device_id=device_id,
                c0=target[0],
                c1=target[1],
                target_mode=str(args.senco6_target_mode),
                c0_decimals=int(args.senco6_c0_decimals),
                c1_decimals=int(args.senco6_c1_decimals),
                ack_timeout_s=float(args.senco6_ack_timeout_s),
            )
            write_applied = True
            base._sleep_gap(max(0.0, float(args.post_write_settle_s)))
            final_live = _read_senco6(
                ga,
                attempts=max(1, int(args.readback_attempts)),
                retry_delay_s=float(args.readback_retry_delay_s),
            )
            if not _matches_senco6(
                target,
                final_live,
                c0_atol=float(args.c0_compare_atol),
                c1_atol=float(args.c1_compare_atol),
                c0_decimals=int(args.senco6_c0_decimals),
                c1_decimals=int(args.senco6_c1_decimals),
            ):
                raise RuntimeError(f"SENCO6_READBACK_MISMATCH target={payload_values} final={final_live}")
            if not acked:
                reason = f"SENCO6_WRITE_ACK_MISSING_BUT_READBACK_MATCHED response={ack_lines}"

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
            "GETCO6_before_live": old_live,
            "GETCO6_after_live": final_live,
            "target_senco6": target,
        }
        rows.append(
            {
                "device_id": device_id,
                "analyzer_prefix": analyzer_cfg.get("name", ""),
                "port": analyzer_cfg.get("port", ""),
                "old_senco6": json.dumps(old_live, ensure_ascii=False),
                "target_senco6": json.dumps(target, ensure_ascii=False),
                "payload_values": json.dumps(payload_values, ensure_ascii=False),
                "payload": payload,
                "final_senco6": json.dumps(final_live, ensure_ascii=False),
                "write_ack_response_json": json.dumps(ack_lines, ensure_ascii=False),
                "status": status,
                "reason": reason,
                "write_applied": write_applied and status.startswith("written_readback_verified"),
                "identity_before": identity_before.get("id", ""),
                "identity_after": identity_after.get("id", ""),
                "runtime_restore_status": restore.get("status", ""),
                "active_freq_restore_status": restore.get("active_freq_restore_status", ""),
                "writes_senco1": False,
                "writes_senco2": False,
                "writes_senco3": False,
                "writes_senco4": False,
                "writes_senco5": False,
                "writes_senco6": status.startswith("written_readback_verified"),
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
        "writes_senco5": False,
        "writes_senco6": True,
        "clears_senco": False,
    }

    base._write_csv(output_dir / "senco6_linear_write_events.csv", rows)
    base._write_csv(output_dir / "conclusion.csv", [conclusion])
    (output_dir / "old_senco6_snapshot.json").write_text(
        json.dumps(old_snapshot, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    (output_dir / "senco6_linear_write_meta.json").write_text(
        json.dumps(
            {
                "created_at": end_ts,
                "tool": "run_v1_5_h2o_senco6_linear_controlled_write",
                "candidate_coefficients_csv": str(candidate_path),
                "main_senco_precheck_dir": str(prewrite_detail.get("precheck_dir") or ""),
                "fit_input_traceability_status": str(prewrite_detail.get("fit_input_traceability_status") or "blocked"),
                "confirmation_text": CONFIRMATION_TEXT,
                "reviewer": args.reviewer,
                "approver": args.approver,
                "writes_senco5": False,
                "writes_senco6": True,
                "writes_device_id": False,
                "clears_senco": False,
                "controls_water_or_gas_routes": False,
                "controls_pace": False,
                "c0_compare_atol": float(args.c0_compare_atol),
                "c1_compare_atol": float(args.c1_compare_atol),
                "senco6_c0_decimals": int(args.senco6_c0_decimals),
                "senco6_c1_decimals": int(args.senco6_c1_decimals),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    metadata = ValidationMetadata(
        tool_name="run_v1_5_h2o_senco6_linear_controlled_write",
        created_at=end_ts,
        analyzers=[str(row.get("device_id") or "") for row in rows],
        input_paths=[str(cfg_path), str(candidate_path), str(prewrite_detail.get("meta_path") or "")],
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
            "Controlled real-device H2O SENCO6 affine trim write.",
            "SENCO6 applies corrected concentration = concentration*C1 + C0.",
            "Payload uses decimal C0/C1, not SENCO2/SENCO4 scientific notation.",
            "C0 and C1 decimal precision are explicit operator-visible parameters; C1 should keep enough digits to preserve multiplier corrections.",
            "No PACE, valve, gas route, water route, humidity generator, device-ID writes, SENCO2/SENCO4 writes, or SENCO5 writes are performed.",
        ],
    )
    write_validation_report(
        output_dir,
        tables={"senco6_linear_write_events": rows, "conclusion": [conclusion]},
        metadata=metadata,
        prefix="senco6_write_report",
    )
    print(json.dumps(conclusion, ensure_ascii=False, indent=2), flush=True)
    return 0 if overall_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
