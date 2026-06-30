"""V1.5 first-discovery SN identity initialization.

This V1.5-owned pre-route layer is intentionally narrower than formal
calibration. It allocates and, when explicitly unlocked, writes an 8-digit
numeric SN/device_code for newly discovered analyzers. It does not write SENCO
coefficients, control routes, sample gas/water points, or fit calibration data.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Optional

from ..devices import GasAnalyzer
from ..tools._analyzer_serial_pacing import (
    MIN_ANALYZER_SERIAL_COMMAND_GAP_S,
    _enforce_serial_command_gap,
)
from ..tools.run_v1_5_analyzer_runtime_setup import (
    _call_optional_ack,
    _default_analyzer_factory,
    _enabled_analyzers,
    _load_json,
    _read_identity_snapshot,
    _read_sn,
)


AUTHORIZATION_PHRASE = "I_AUTHORIZE_V1_5_SN_IDENTITY_WRITE"
DEFAULT_OUTPUT_ROOT = Path("_handoff") / "v1_5_sn_identity_initialization"
AnalyzerFactory = Callable[[Mapping[str, Any]], Any]
SN_RE = re.compile(r"\d{8}")
PROTOCOL_ID_RE = re.compile(r"\d{3}")


class SnIdentityInitializationError(RuntimeError):
    """Raised when SN identity initialization cannot proceed safely."""


def _now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def _valid_bound_sn(value: Any) -> bool:
    text = str(value or "").strip()
    return bool(SN_RE.fullmatch(text)) and text != "00000000"


def _year_month() -> str:
    return datetime.now().strftime("%y%m")


def _sn_policy(config: Mapping[str, Any]) -> dict[str, Any]:
    raw = dict(config.get("sn_identity_contract") or config.get("sn_allocation_contract") or {})
    hardware_version_prefix = str(raw.get("hardware_version_prefix", "01")).strip()
    year_month = str(raw.get("year_month", _year_month())).strip()
    if not re.fullmatch(r"\d{2}", hardware_version_prefix):
        raise SnIdentityInitializationError("hardware_version_prefix must be 2 numeric digits")
    if not re.fullmatch(r"\d{4}", year_month):
        raise SnIdentityInitializationError("year_month must be YYMM numeric digits")
    raw_write_target = str(raw.get("write_target", raw.get("write_target_id", "FFF"))).strip()
    write_target = raw_write_target.upper()
    if write_target == "PROTOCOL_DEVICE_ID":
        write_target = "protocol_device_id"
    if write_target not in {"FFF", "protocol_device_id"} and not PROTOCOL_ID_RE.fullmatch(write_target):
        raise SnIdentityInitializationError("write_target must be FFF, protocol_device_id, or a 3-digit target")
    return {
        "hardware_version_prefix": hardware_version_prefix,
        "year_month": year_month,
        "sequence_start": int(raw.get("sequence_start", 1)),
        "qr_prefix": str(raw.get("qr_prefix", "GCA1")).strip() or "GCA1",
        "write_target": write_target,
        "allow_existing_sn_rewrite": bool(raw.get("allow_existing_sn_rewrite", False)),
        "command_gap_s": max(MIN_ANALYZER_SERIAL_COMMAND_GAP_S, float(raw.get("command_gap_s", 1.2))),
        "sn_read_timeout_s": float(raw.get("sn_read_timeout_s", 1.2)),
        "mode2_verify_required": bool(raw.get("mode2_verify_required", True)),
    }


def _existing_sn_codes(config: Mapping[str, Any], explicit: Iterable[str] | None = None) -> set[str]:
    values: set[str] = set()
    for item in explicit or []:
        text = str(item or "").strip()
        if text:
            values.add(text)
    for item in config.get("existing_sn_codes") or []:
        text = str(item or "").strip()
        if text:
            values.add(text)
    for item in config.get("reserved_sn_codes") or []:
        text = str(item or "").strip()
        if text:
            values.add(text)
    return values


def _next_candidate_sn(policy: Mapping[str, Any], used: set[str]) -> str:
    prefix = f"{policy['hardware_version_prefix']}{policy['year_month']}"
    sequence = int(policy["sequence_start"])
    for number in range(sequence, 100):
        candidate = f"{prefix}{number:02d}"
        if candidate not in used and candidate != "00000000":
            return candidate
    raise SnIdentityInitializationError(f"no available SN sequence for prefix {prefix}")


def _write_target_id(row: Mapping[str, Any], policy: Mapping[str, Any]) -> str:
    target = str(row.get("write_target") or policy.get("write_target") or "FFF").strip().upper()
    if target == "PROTOCOL_DEVICE_ID" or target == "protocol_device_id":
        return str(row.get("protocol_device_id") or "").strip()
    return target


def build_sn_identity_initialization_plan(
    config: Mapping[str, Any],
    *,
    existing_sn_codes: Iterable[str] | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    analyzers = _enabled_analyzers(config)
    policy = _sn_policy(config)
    used = _existing_sn_codes(config, existing_sn_codes)
    plan_rows: list[dict[str, Any]] = []
    issues: list[dict[str, Any]] = []
    for index, item in enumerate(analyzers, start=1):
        slot = str(item.get("slot") or f"GA{index:02d}").strip()
        port = str(item.get("port") or "").strip()
        protocol_id = str(item.get("protocol_device_id") or "").strip()
        current_sn = str(item.get("sn_code") or item.get("current_sn") or "").strip()
        requested_sn = str(item.get("desired_sn") or item.get("new_sn") or item.get("allocated_sn") or "").strip()
        row_issues: list[str] = []
        if not port:
            row_issues.append("missing_port")
        if not PROTOCOL_ID_RE.fullmatch(protocol_id):
            row_issues.append("protocol_device_id_not_3_digits")

        current_bound = _valid_bound_sn(current_sn)
        if requested_sn:
            target_sn = requested_sn
        elif current_bound:
            target_sn = current_sn
        else:
            target_sn = _next_candidate_sn(policy, used)

        if not SN_RE.fullmatch(target_sn):
            row_issues.append("target_sn_not_8_numeric_digits")
        elif target_sn == "00000000":
            row_issues.append("target_sn_uninitialized_00000000")
        if target_sn in used and target_sn != current_sn:
            row_issues.append("target_sn_already_reserved")
        used.add(target_sn)

        if current_bound and target_sn != current_sn and not bool(policy["allow_existing_sn_rewrite"]):
            row_issues.append("existing_sn_rewrite_requires_maintenance_policy")

        write_required = not current_bound or target_sn != current_sn
        write_target_id = _write_target_id({"protocol_device_id": protocol_id, **dict(item)}, policy)
        if write_target_id == "PROTOCOL_DEVICE_ID":
            write_target_id = protocol_id
        if write_required and not (write_target_id == "FFF" or PROTOCOL_ID_RE.fullmatch(write_target_id)):
            row_issues.append("write_target_id_invalid")

        status = "blocked" if row_issues else ("write_required" if write_required else "already_bound_no_write")
        for code in row_issues:
            issues.append({"slot": slot, "port": port, "protocol_device_id": protocol_id, "code": code})
        plan_rows.append(
            {
                "index": index,
                "slot": slot,
                "port": port,
                "baud": int(item.get("baud", 115200)),
                "protocol_device_id": protocol_id,
                "usb_serial_number": item.get("usb_serial_number") or item.get("usb_serial"),
                "current_sn_expected": current_sn,
                "target_sn": target_sn,
                "device_code": target_sn,
                "hardware_version_prefix": target_sn[:2] if SN_RE.fullmatch(target_sn) else "",
                "year_month": target_sn[2:6] if SN_RE.fullmatch(target_sn) else "",
                "sequence": target_sn[6:8] if SN_RE.fullmatch(target_sn) else "",
                "qr_payload": f"{policy['qr_prefix']}:{target_sn}",
                "write_required": bool(write_required),
                "write_target_id": write_target_id,
                "write_command": f"SN,YGAS,{write_target_id},{target_sn}" if write_required else "",
                "status": status,
                "issues": row_issues,
            }
        )

    status = "blocked" if issues else ("write_required" if any(row["write_required"] for row in plan_rows) else "already_bound")
    return {
        "schema_version": "v1_5_sn_identity_initialization_plan_v0",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "run_id": str(run_id or f"v1_5_sn_identity_initialization_plan_{_now_stamp()}"),
        "status": status,
        "analyzer_count": len(plan_rows),
        "write_candidate_count": len([row for row in plan_rows if row.get("write_required")]),
        "policy": policy,
        "rows": plan_rows,
        "issues": issues,
        "boundary": {
            "opens_com_when_executed": True,
            "writes_sn": True,
            "writes_device_id": False,
            "writes_senco": False,
            "controls_gas_route": False,
            "controls_water_route": False,
            "controls_pressure": False,
            "controls_temperature": False,
            "runs_sampling": False,
            "runs_fitting": False,
            "execute_requires_operator_confirmation": True,
            "serial_execution": True,
            "serial_command_min_gap_s": MIN_ANALYZER_SERIAL_COMMAND_GAP_S,
        },
    }


def _write_plan_outputs(plan: Mapping[str, Any], output_dir: str | Path) -> dict[str, str]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    plan_json = out_dir / "v1_5_sn_identity_initialization_plan.json"
    plan_csv = out_dir / "v1_5_sn_identity_initialization_plan.csv"
    summary_md = out_dir / "V1_5_SN_IDENTITY_INITIALIZATION_PLAN.md"
    _write_json(plan_json, plan)
    fieldnames = [
        "index",
        "slot",
        "port",
        "protocol_device_id",
        "current_sn_expected",
        "target_sn",
        "device_code",
        "write_required",
        "write_target_id",
        "write_command",
        "status",
        "issues",
    ]
    with plan_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in plan.get("rows") or []:
            payload = {key: row.get(key, "") for key in fieldnames}
            payload["issues"] = ";".join(str(item) for item in row.get("issues") or [])
            writer.writerow(payload)
    lines = [
        "# V1.5 SN Identity Initialization Plan",
        "",
        f"- status: {plan.get('status')}",
        f"- analyzer_count: {plan.get('analyzer_count')}",
        f"- write_candidate_count: {plan.get('write_candidate_count')}",
        "- default: dry-run only; execution requires explicit operator confirmation.",
        "- boundary: SN/device_code only; no SENCO, route, pressure, temperature, sampling, or fitting.",
        "",
        "## Rows",
    ]
    for row in plan.get("rows") or []:
        lines.append(
            f"- {row.get('slot')} {row.get('port')} ID={row.get('protocol_device_id')} "
            f"{row.get('current_sn_expected')} -> {row.get('target_sn')} status={row.get('status')}"
        )
    summary_md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"plan_json": str(plan_json), "plan_csv": str(plan_csv), "summary_md": str(summary_md)}


def _write_result_outputs(result: Mapping[str, Any], output_dir: str | Path) -> dict[str, str]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    result_json = out_dir / "v1_5_sn_identity_initialization_result.json"
    detail_csv = out_dir / "v1_5_sn_identity_initialization_result.csv"
    summary_md = out_dir / "V1_5_SN_IDENTITY_INITIALIZATION_RESULT.md"
    _write_json(result_json, result)
    fieldnames = [
        "slot",
        "port",
        "protocol_device_id",
        "current_sn_expected",
        "target_sn",
        "pre_sn",
        "readback1_sn",
        "readback2_sn",
        "status",
        "error",
    ]
    with detail_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in result.get("results") or []:
            writer.writerow({key: row.get(key, "") for key in fieldnames})
    lines = [
        "# V1.5 SN Identity Initialization Result",
        "",
        f"- status: {result.get('status')}",
        f"- execute: {result.get('execute')}",
        f"- write_candidate_count: {result.get('write_candidate_count')}",
        f"- completed_write_count: {result.get('completed_write_count')}",
        "- boundary: SN/device_code only; no SENCO, route, pressure, temperature, sampling, or fitting.",
        "",
        "## Rows",
    ]
    for row in result.get("results") or []:
        lines.append(f"- {row.get('slot')}: {row.get('status')} {row.get('pre_sn')} -> {row.get('target_sn')}")
    summary_md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"result_json": str(result_json), "detail_csv": str(detail_csv), "summary_md": str(summary_md)}


def _send_sn_write(analyzer: Any, command: str) -> bool:
    sender = getattr(analyzer, "_send_config_with_retries", None)
    if callable(sender):
        try:
            return bool(sender(command, broadcast=True, require_ack=False, attempts=1))
        except TypeError:
            return bool(sender(command, broadcast=True, require_ack=False))
    ser = getattr(analyzer, "ser", None)
    if ser is None or not callable(getattr(ser, "write", None)):
        raise SnIdentityInitializationError("analyzer has no writable serial object")
    ser.write(command + "\r\n")
    return True


def execute_sn_identity_initialization(
    plan: Mapping[str, Any],
    *,
    output_dir: str | Path,
    analyzer_factory: Optional[AnalyzerFactory] = None,
    execute: bool = False,
    acknowledge_sn_write: bool = False,
    run_id: str | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    plan_paths = _write_plan_outputs(plan, out_dir / "00_plan")
    write_rows = [dict(row) for row in plan.get("rows") or [] if bool(row.get("write_required"))]
    resolved_run_id = str(run_id or f"v1_5_sn_identity_initialization_{'real' if execute else 'dry_run'}_{_now_stamp()}")
    if str(plan.get("status")) == "blocked":
        result = {
            "schema_version": "v1_5_sn_identity_initialization_result_v0",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "run_id": resolved_run_id,
            "status": "blocked_plan_not_safe",
            "execute": bool(execute),
            "write_candidate_count": len(write_rows),
            "completed_write_count": 0,
            "plan": plan,
            "results": [],
            "evidence_paths": {f"plan_{key}": value for key, value in plan_paths.items()},
            "boundary": plan.get("boundary"),
        }
        result_paths = _write_result_outputs(result, out_dir)
        result["evidence_paths"].update({f"result_{key}": value for key, value in result_paths.items()})
        _write_json(Path(result_paths["result_json"]), result)
        return result
    if not execute:
        result = {
            "schema_version": "v1_5_sn_identity_initialization_result_v0",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "run_id": resolved_run_id,
            "status": "dry_run_ready",
            "execute": False,
            "write_candidate_count": len(write_rows),
            "completed_write_count": 0,
            "plan": plan,
            "results": [],
            "evidence_paths": {f"plan_{key}": value for key, value in plan_paths.items()},
            "boundary": plan.get("boundary"),
        }
        result_paths = _write_result_outputs(result, out_dir)
        result["evidence_paths"].update({f"result_{key}": value for key, value in result_paths.items()})
        _write_json(Path(result_paths["result_json"]), result)
        return result
    if write_rows and not acknowledge_sn_write:
        result = {
            "schema_version": "v1_5_sn_identity_initialization_result_v0",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "run_id": resolved_run_id,
            "status": "blocked_requires_sn_write_ack",
            "execute": True,
            "write_candidate_count": len(write_rows),
            "completed_write_count": 0,
            "plan": plan,
            "results": [],
            "evidence_paths": {f"plan_{key}": value for key, value in plan_paths.items()},
            "boundary": plan.get("boundary"),
        }
        result_paths = _write_result_outputs(result, out_dir)
        result["evidence_paths"].update({f"result_{key}": value for key, value in result_paths.items()})
        _write_json(Path(result_paths["result_json"]), result)
        return result

    factory = analyzer_factory or _default_analyzer_factory
    results: list[dict[str, Any]] = []
    policy = dict(plan.get("policy") or {})
    command_gap_s = policy.get("command_gap_s", MIN_ANALYZER_SERIAL_COMMAND_GAP_S)
    for row in write_rows:
        item = dict(row)
        item["baud"] = int(item.get("baud", 115200))
        result_row = {
            "slot": item.get("slot"),
            "port": item.get("port"),
            "protocol_device_id": item.get("protocol_device_id"),
            "current_sn_expected": item.get("current_sn_expected"),
            "target_sn": item.get("target_sn"),
            "write_command_sent": item.get("write_command"),
            "status": "started",
            "events": [],
        }
        analyzer = factory(item)
        try:
            opener = getattr(analyzer, "open", None)
            if callable(opener):
                opener()
            with _enforce_serial_command_gap(analyzer, command_gap_s, sleep_fn=sleep_fn) as pacing_events:
                result_row["serial_command_min_gap_s"] = MIN_ANALYZER_SERIAL_COMMAND_GAP_S
                result_row["serial_command_pacing_events"] = pacing_events
                pre_sn, pre_raw = _read_sn(analyzer, timeout_s=float(policy.get("sn_read_timeout_s", 1.2)))
                result_row["pre_sn"] = pre_sn
                result_row["pre_sn_raw"] = pre_raw
                expected = str(item.get("current_sn_expected") or "").strip()
                if expected and pre_sn and pre_sn != expected:
                    raise SnIdentityInitializationError(f"pre-write SN mismatch read={pre_sn} expected={expected}")
                identity = _read_identity_snapshot(analyzer, prefer_stream=False)
                result_row["identity_before_write"] = identity
                expected_id = str(item.get("protocol_device_id") or "")
                if isinstance(identity, Mapping) and identity.get("id") and str(identity.get("id")) != expected_id:
                    raise SnIdentityInitializationError(
                        f"protocol id mismatch read={identity.get('id')} expected={expected_id}"
                    )
                mode_ok = _call_optional_ack(getattr(analyzer, "set_mode_with_ack", None), 2)
                result_row["events"].append({"action": "set_mode2", "ok": bool(mode_ok)})
                if bool(policy.get("mode2_verify_required", True)) and not mode_ok:
                    raise SnIdentityInitializationError("MODE2 was not acknowledged before SN write")
                if not _send_sn_write(analyzer, str(item.get("write_command") or "")):
                    raise SnIdentityInitializationError("SN write command was not accepted by sender")
                result_row["events"].append({"action": "write_sn", "ok": True})
                readback1, raw1 = _read_sn(analyzer, timeout_s=float(policy.get("sn_read_timeout_s", 1.2)))
                readback2, raw2 = _read_sn(analyzer, timeout_s=float(policy.get("sn_read_timeout_s", 1.2)))
                result_row["readback1_sn"] = readback1
                result_row["readback1_raw"] = raw1
                result_row["readback2_sn"] = readback2
                result_row["readback2_raw"] = raw2
                if readback1 != item.get("target_sn") or readback2 != item.get("target_sn"):
                    raise SnIdentityInitializationError(
                        f"SN readback mismatch readback1={readback1} readback2={readback2} expected={item.get('target_sn')}"
                    )
                result_row["status"] = "sn_write_verified_mode2"
        except Exception as exc:
            result_row["status"] = "failed"
            result_row["error"] = str(exc)
        finally:
            closer = getattr(analyzer, "close", None)
            if callable(closer):
                try:
                    closer()
                except Exception:
                    pass
        results.append(result_row)

    success = len(results) == len(write_rows) and all(row.get("status") == "sn_write_verified_mode2" for row in results)
    result = {
        "schema_version": "v1_5_sn_identity_initialization_result_v0",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "run_id": resolved_run_id,
        "status": "success" if success else "partial",
        "execute": True,
        "write_candidate_count": len(write_rows),
        "completed_write_count": len([row for row in results if row.get("status") == "sn_write_verified_mode2"]),
        "plan": plan,
        "results": results,
        "evidence_paths": {f"plan_{key}": value for key, value in plan_paths.items()},
        "boundary": plan.get("boundary"),
    }
    result_paths = _write_result_outputs(result, out_dir)
    result["evidence_paths"].update({f"result_{key}": value for key, value in result_paths.items()})
    _write_json(Path(result_paths["result_json"]), result)
    return result


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plan or execute V1.5 first-discovery SN identity initialization.")
    parser.add_argument("--config", required=True, help="V1.5 identity initialization JSON config.")
    parser.add_argument("--output-dir", default=None, help="Output directory. Defaults under _handoff.")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--execute", action="store_true", help="Open COM and write SN for write-required rows.")
    parser.add_argument(
        "--operator-confirm",
        default="",
        help=f"Required with --execute. Exact phrase: {AUTHORIZATION_PHRASE}",
    )
    parser.add_argument("--existing-sn", action="append", default=[], help="Reserve an existing SN code; may repeat.")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    output_dir = Path(args.output_dir) if args.output_dir else DEFAULT_OUTPUT_ROOT / f"run_{_now_stamp()}"
    try:
        config = _load_json(args.config)
        if args.execute and str(args.operator_confirm or "").strip() != AUTHORIZATION_PHRASE:
            raise SnIdentityInitializationError(f"--execute requires --operator-confirm {AUTHORIZATION_PHRASE!r}")
        plan = build_sn_identity_initialization_plan(config, existing_sn_codes=args.existing_sn, run_id=args.run_id)
        result = execute_sn_identity_initialization(
            plan,
            output_dir=output_dir,
            execute=bool(args.execute),
            acknowledge_sn_write=bool(args.execute),
            run_id=args.run_id,
        )
        print(
            json.dumps(
                {
                    "status": result.get("status"),
                    "run_id": result.get("run_id"),
                    "execute": result.get("execute"),
                    "output_dir": str(output_dir),
                    "write_candidate_count": result.get("write_candidate_count"),
                    "completed_write_count": result.get("completed_write_count"),
                    "result_json": result.get("evidence_paths", {}).get("result_result_json"),
                },
                ensure_ascii=False,
                indent=2,
            ),
            flush=True,
        )
        return 0 if result.get("status") in {"dry_run_ready", "success"} else 1
    except Exception as exc:
        print(str(exc), file=sys.stderr, flush=True)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
