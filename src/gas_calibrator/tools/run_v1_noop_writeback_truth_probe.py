from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

from .run_v1_corrected_autodelivery import (
    _normalize_analyzer,
    _normalize_device_id,
    _parse_senco_command,
    write_coefficients_to_live_devices,
)


def _read_csv(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _write_markdown(path: Path, lines: Sequence[str]) -> None:
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def _build_minimal_live_cfg(
    *,
    analyzer: str,
    port: str,
    baudrate: int,
    timeout: float,
    device_id: str,
    active_send: bool,
    ftd_hz: int,
    average_filter: int,
) -> Dict[str, Any]:
    return {
        "devices": {
            "gas_analyzers": [
                {
                    "name": analyzer,
                    "port": str(port),
                    "baud": int(baudrate),
                    "timeout": float(timeout),
                    "device_id": str(device_id),
                    "active_send": bool(active_send),
                    "ftd_hz": int(ftd_hz),
                    "average_filter": int(average_filter),
                    "enabled": True,
                }
            ]
        }
    }


def _collect_expected_groups(
    *,
    analyzer: str,
    download_plan_rows: Sequence[Mapping[str, Any]],
    temperature_rows: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    groups: List[Dict[str, Any]] = []
    for row in download_plan_rows:
        if _normalize_analyzer(row.get("Analyzer")) != analyzer:
            continue
        for key in ("PrimaryCommand", "SecondaryCommand"):
            command = str(row.get(key) or "").strip()
            if not command:
                continue
            group, coeffs = _parse_senco_command(command)
            groups.append(
                {
                    "group": int(group),
                    "command": command,
                    "coefficients": [float(value) for value in coeffs],
                    "source_file": "download_plan_no_500.csv",
                    "gas": str(row.get("Gas") or ""),
                    "channel": str(row.get(key.replace("Command", "SENCO")) or ""),
                }
            )
    for row in temperature_rows:
        if _normalize_analyzer(row.get("analyzer_id")) != analyzer:
            continue
        command = str(row.get("command_string") or "").strip()
        if not command:
            continue
        group, coeffs = _parse_senco_command(command)
        groups.append(
            {
                "group": int(group),
                "command": command,
                "coefficients": [float(value) for value in coeffs],
                "source_file": "temperature_coefficients_target.csv",
                "gas": str(row.get("fit_type") or ""),
                "channel": str(row.get("senco_channel") or ""),
            }
        )
    groups.sort(key=lambda item: int(item["group"]))
    return groups


def _build_session_sequence(groups: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    sequence: List[Dict[str, Any]] = [
        {
            "phase": "prepare",
            "action": "set_comm_way_false_noack",
            "safe_replay": "controlled_only",
            "prime_candidate": True,
            "persistent_write": True,
            "note": "Outer writeback session quiets active stream before any GETCO/SENCO.",
        }
    ]
    for item in groups:
        group = int(item.get("group") or 0)
        sequence.append(
            {
                "phase": "prewrite",
                "action": f"getco_before_group_{group}",
                "group": group,
                "safe_replay": "read_only_controlled_only",
                "prime_candidate": True,
                "persistent_write": False,
                "note": "Each GETCO also triggers an internal SETCOMWAY 0 + quiet delay + flush via _prepare_coefficient_io().",
            }
        )
    sequence.append(
        {
            "phase": "writeback",
            "action": "set_mode_2",
            "safe_replay": "controlled_only",
            "prime_candidate": True,
            "persistent_write": True,
            "note": "Calibration-mode transition immediately precedes SENCO writes in the historical path.",
        }
    )
    for item in groups:
        group = int(item.get("group") or 0)
        sequence.append(
            {
                "phase": "writeback",
                "action": f"set_senco_group_{group}",
                "group": group,
                "safe_replay": "no_op_only_with_approval",
                "prime_candidate": True,
                "persistent_write": True,
                "command": str(item.get("command") or ""),
                "note": "Each SENCO write also re-enters _prepare_coefficient_io() before payload transmission.",
            }
        )
        sequence.append(
            {
                "phase": "verify",
                "action": f"getco_after_write_group_{group}",
                "group": group,
                "safe_replay": "read_only_controlled_only",
                "prime_candidate": True,
                "persistent_write": False,
                "note": "Readback must be explicit C0 to count as verified.",
            }
        )
    sequence.extend(
        [
            {
                "phase": "restore",
                "action": "set_mode_1",
                "safe_replay": "controlled_only",
                "prime_candidate": False,
                "persistent_write": True,
                "note": "write_senco_groups_with_full_verification exits the calibration write phase back to mode 1 before restore helper runs.",
            },
            {
                "phase": "restore",
                "action": "restore_mode_2",
                "safe_replay": "controlled_only",
                "prime_candidate": False,
                "persistent_write": True,
                "note": "Existing V1 restore helper re-applies mode 2 before restoring FTD/average/comm-way.",
            },
            {
                "phase": "restore",
                "action": "restore_ftd",
                "safe_replay": "controlled_only",
                "prime_candidate": False,
                "persistent_write": True,
            },
            {
                "phase": "restore",
                "action": "restore_average_filter",
                "safe_replay": "controlled_only",
                "prime_candidate": False,
                "persistent_write": True,
            },
            {
                "phase": "restore",
                "action": "restore_comm_way",
                "safe_replay": "controlled_only",
                "prime_candidate": False,
                "persistent_write": True,
            },
        ]
    )
    return sequence


def _render_plan_markdown(plan: Mapping[str, Any]) -> List[str]:
    device = dict(plan.get("device") or {})
    readiness = dict(plan.get("readiness") or {})
    groups = list(plan.get("groups") or [])
    protections = list(plan.get("protections_required") or [])
    risks = list(plan.get("risks") or [])
    sequence = list(plan.get("session_sequence") or [])

    lines = [
        "# noop writeback truth probe plan",
        "",
        "## scope",
        f"- candidate_dir: {plan.get('candidate_dir')}",
        f"- output_dir: {plan.get('output_dir')}",
        f"- analyzer: {device.get('analyzer')}",
        f"- device_id: {device.get('device_id')}",
        f"- port: {device.get('port')}",
        f"- execute_requested: {bool(plan.get('execute_requested'))}",
        f"- dry_run: {bool(plan.get('dry_run'))}",
        "",
        "## readiness",
        f"- final_write_ready: {readiness.get('final_write_ready')}",
        f"- readiness_code: {readiness.get('readiness_code')}",
        f"- readiness_reason: {readiness.get('readiness_reason')}",
        "",
        "## same_value_assessment",
        f"- confirmed: {bool(plan.get('same_value_confirmed'))}",
        f"- reason: {plan.get('same_value_reason')}",
        f"- allowed_to_execute: {bool(plan.get('allowed_to_execute'))}",
        f"- execution_block_reason: {plan.get('execution_block_reason')}",
        "",
        "## groups",
    ]
    if groups:
        for item in groups:
            lines.append(
                f"- group={item.get('group')} channel={item.get('channel')} source={item.get('source_file')} command={item.get('command')}"
            )
    else:
        lines.append("- none")

    lines.extend(["", "## protections_required"])
    if protections:
        for item in protections:
            lines.append(f"- {item}")
    else:
        lines.append("- none")

    lines.extend(["", "## risks"])
    if risks:
        for item in risks:
            lines.append(f"- {item}")
    else:
        lines.append("- none")

    lines.extend(["", "## session_sequence"])
    for item in sequence:
        lines.append(
            f"- phase={item.get('phase')} action={item.get('action')} group={item.get('group', '')} prime={item.get('prime_candidate')} persistent_write={item.get('persistent_write')} safe_replay={item.get('safe_replay')}"
        )
        note = str(item.get("note") or "").strip()
        if note:
            lines.append(f"  note: {note}")

    return lines


def build_noop_writeback_truth_plan(
    *,
    candidate_dir: str | Path,
    device_id: str,
    port: str,
    baudrate: int = 115200,
    timeout: float = 0.6,
    active_send: bool = True,
    ftd_hz: int = 10,
    average_filter: int = 49,
    output_dir: str | Path | None = None,
    execute_requested: bool = False,
    assume_same_value_confirmed: bool = False,
    allow_unconfirmed_same_value: bool = False,
) -> Dict[str, Any]:
    candidate_dir = Path(candidate_dir).resolve()
    output_dir = Path(output_dir).resolve() if output_dir else candidate_dir / f"noop_writeback_truth_probe_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    output_dir.mkdir(parents=True, exist_ok=True)

    normalized_device_id = _normalize_device_id(device_id)
    download_plan_rows = _read_csv(candidate_dir / "download_plan_no_500.csv")
    temperature_rows = _read_csv(candidate_dir / "temperature_coefficients_target.csv")
    readiness_rows = _read_csv(candidate_dir / "write_readiness_summary.csv") if (candidate_dir / "write_readiness_summary.csv").exists() else []
    readiness_row = dict(readiness_rows[0]) if readiness_rows else {}

    device_rows = [
        row
        for row in download_plan_rows
        if _normalize_device_id(row.get("ActualDeviceId")) == normalized_device_id
    ]
    analyzers = sorted({_normalize_analyzer(row.get("Analyzer")) for row in device_rows if _normalize_analyzer(row.get("Analyzer"))})
    if len(analyzers) != 1:
        raise RuntimeError(f"expected exactly one analyzer for device_id={normalized_device_id}, got {analyzers}")
    analyzer = analyzers[0]
    filtered_temperature_rows = [
        row for row in temperature_rows if _normalize_analyzer(row.get("analyzer_id")) == analyzer
    ]
    groups = _collect_expected_groups(
        analyzer=analyzer,
        download_plan_rows=device_rows,
        temperature_rows=filtered_temperature_rows,
    )
    if not groups:
        raise RuntimeError(f"no candidate groups found for analyzer={analyzer} device_id={normalized_device_id}")

    same_value_confirmed = bool(assume_same_value_confirmed)
    same_value_reason = (
        "operator asserted that current live coefficients already equal the candidate set"
        if same_value_confirmed
        else "current explicit-C0 backup is still unavailable, so same-value status cannot be proven from live readback"
    )
    allowed_to_execute = bool(execute_requested) and (same_value_confirmed or bool(allow_unconfirmed_same_value))
    if execute_requested and not same_value_confirmed and not bool(allow_unconfirmed_same_value):
        execution_block_reason = "same-value is unconfirmed; refusing live no-op write until operator explicitly accepts that risk"
    elif execute_requested and not same_value_confirmed and bool(allow_unconfirmed_same_value):
        execution_block_reason = ""
    elif execute_requested:
        execution_block_reason = ""
    else:
        execution_block_reason = "execute flag not provided"

    protections_required = [
        "Limit scope to 079 on COM39 only; do not scan or touch any other device.",
        "Persist full raw transcript for prepare / mode / SENCO / GETCO / restore.",
        "Require explicit C0 for verified=true; ambiguous and none remain unverified.",
        "Freeze candidate inputs from the current no_500 candidate directory before any live action.",
        "Abort if scanned live device_id is not exactly 079.",
        "Do not include pressure group 9 in this truth probe.",
        "Do not auto-run unless the operator passes --execute and explicitly confirms same-value risk.",
    ]
    risks = [
        "SETCOMWAY / MODE / FTD / average restore steps are real device state writes even in a no-op coefficient session.",
        "SENCO writes are still persistent write commands; same-value intent reduces but does not eliminate risk.",
        "Current standalone probes cannot prove that the live device already equals the candidate coefficients.",
        "Historical 6/6 evidence is insufficient under the explicit-C0 truth standard.",
    ]
    session_sequence = _build_session_sequence(groups)
    cfg = _build_minimal_live_cfg(
        analyzer=analyzer,
        port=port,
        baudrate=baudrate,
        timeout=timeout,
        device_id=normalized_device_id,
        active_send=active_send,
        ftd_hz=ftd_hz,
        average_filter=average_filter,
    )
    plan: Dict[str, Any] = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "candidate_dir": str(candidate_dir),
        "output_dir": str(output_dir),
        "device": {
            "analyzer": analyzer,
            "device_id": normalized_device_id,
            "port": str(port),
            "baudrate": int(baudrate),
            "timeout": float(timeout),
            "active_send": bool(active_send),
            "ftd_hz": int(ftd_hz),
            "average_filter": int(average_filter),
        },
        "readiness": readiness_row,
        "groups": groups,
        "group_ids": [int(item["group"]) for item in groups],
        "pressure_groups_included": False,
        "execute_requested": bool(execute_requested),
        "dry_run": not bool(execute_requested),
        "same_value_confirmed": bool(same_value_confirmed),
        "same_value_reason": same_value_reason,
        "allow_unconfirmed_same_value": bool(allow_unconfirmed_same_value),
        "allowed_to_execute": bool(allowed_to_execute),
        "execution_block_reason": execution_block_reason,
        "protections_required": protections_required,
        "risks": risks,
        "session_sequence": session_sequence,
        "planned_cfg": cfg,
        "planned_artifacts": [
            "noop_writeback_truth_plan.json",
            "noop_writeback_truth_plan.md",
            "writeback_raw_transcript.log",
            "writeback_truth_summary.json",
            "writeback_truth_groups.csv",
        ],
    }
    _write_json(output_dir / "noop_writeback_truth_plan.json", plan)
    _write_markdown(output_dir / "noop_writeback_truth_plan.md", _render_plan_markdown(plan))
    return plan


def run_from_cli(
    *,
    candidate_dir: str | Path,
    device_id: str,
    port: str,
    baudrate: int = 115200,
    timeout: float = 0.6,
    active_send: bool = True,
    ftd_hz: int = 10,
    average_filter: int = 49,
    output_dir: str | Path | None = None,
    execute: bool = False,
    assume_same_value_confirmed: bool = False,
    allow_unconfirmed_same_value: bool = False,
) -> Dict[str, Any]:
    plan = build_noop_writeback_truth_plan(
        candidate_dir=candidate_dir,
        device_id=device_id,
        port=port,
        baudrate=baudrate,
        timeout=timeout,
        active_send=active_send,
        ftd_hz=ftd_hz,
        average_filter=average_filter,
        output_dir=output_dir,
        execute_requested=execute,
        assume_same_value_confirmed=assume_same_value_confirmed,
        allow_unconfirmed_same_value=allow_unconfirmed_same_value,
    )
    result: Dict[str, Any] = {
        "plan": plan,
        "plan_json_path": str(Path(plan["output_dir"]) / "noop_writeback_truth_plan.json"),
        "plan_markdown_path": str(Path(plan["output_dir"]) / "noop_writeback_truth_plan.md"),
        "executed": False,
        "write_result": {},
    }
    if not execute:
        return result
    if not bool(plan.get("allowed_to_execute")):
        result["execution_blocked"] = True
        result["execution_block_reason"] = str(plan.get("execution_block_reason") or "execution not allowed")
        return result

    filtered_download_plan_rows = [
        row
        for row in _read_csv(Path(candidate_dir) / "download_plan_no_500.csv")
        if _normalize_device_id(row.get("ActualDeviceId")) == _normalize_device_id(device_id)
    ]
    filtered_temperature_rows = [
        row
        for row in _read_csv(Path(candidate_dir) / "temperature_coefficients_target.csv")
        if _normalize_analyzer(row.get("analyzer_id")) == plan["device"]["analyzer"]
    ]
    write_result = write_coefficients_to_live_devices(
        cfg=dict(plan.get("planned_cfg") or {}),
        output_dir=plan["output_dir"],
        download_plan_rows=filtered_download_plan_rows,
        temperature_rows=filtered_temperature_rows,
        pressure_rows=[],
        actual_device_ids={str(plan["device"]["analyzer"]): str(plan["device"]["device_id"])},
        write_pressure_rows=False,
    )
    result["executed"] = True
    result["write_result"] = write_result
    return result


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Prepare or execute a controlled V1 no-op writeback truth probe.")
    parser.add_argument("--candidate-dir", required=True)
    parser.add_argument("--device-id", required=True)
    parser.add_argument("--port", required=True)
    parser.add_argument("--baudrate", type=int, default=115200)
    parser.add_argument("--timeout", type=float, default=0.6)
    parser.add_argument("--active-send", dest="active_send", action="store_true")
    parser.add_argument("--passive-send", dest="active_send", action="store_false")
    parser.set_defaults(active_send=True)
    parser.add_argument("--ftd-hz", type=int, default=10)
    parser.add_argument("--average-filter", type=int, default=49)
    parser.add_argument("--output-dir")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--assume-same-value-confirmed", action="store_true")
    parser.add_argument("--allow-unconfirmed-same-value", action="store_true")
    args = parser.parse_args(argv)

    execute = bool(args.execute)
    if args.dry_run and execute:
        raise SystemExit("--dry-run and --execute cannot be used together")

    result = run_from_cli(
        candidate_dir=args.candidate_dir,
        device_id=args.device_id,
        port=args.port,
        baudrate=int(args.baudrate),
        timeout=float(args.timeout),
        active_send=bool(args.active_send),
        ftd_hz=int(args.ftd_hz),
        average_filter=int(args.average_filter),
        output_dir=args.output_dir,
        execute=execute,
        assume_same_value_confirmed=bool(args.assume_same_value_confirmed),
        allow_unconfirmed_same_value=bool(args.allow_unconfirmed_same_value),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
