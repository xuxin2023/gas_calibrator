"""Blocked guard for V1.5 SENCO7/SENCO8 candidate temperature writes.

V1.5 production keeps analyzer temperature coefficients neutral by default.
This compatibility entrypoint exists so stale handoff scripts fail safely:
it reads the candidate review CSV, writes a blocked evidence sidecar, and never
opens COM ports or writes coefficients.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence


CONFIRMATION_TEXT = "WRITE_SENCO78_CANDIDATE_V1_5_TEMPERATURE_INPUTS"
SUPPORTED_CHANNELS = (7, 8)
BLOCK_REASON = "temperature_coefficients_remain_neutral_by_v1_5_policy"


def _device_id(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.startswith("GA"):
        text = text[2:]
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _parse_channel(value: Any) -> int:
    text = str(value or "").strip().upper().replace("SENCO", "")
    group = int(text)
    if group not in SUPPORTED_CHANNELS:
        raise ValueError(f"Temperature SENCO group must be 7 or 8, got {value!r}")
    return group


def _parse_values(value: Any) -> List[float]:
    text = str(value or "").strip()
    if not text:
        return []
    numbers = re.findall(r"[-+]?\d+(?:\.\d+)?(?:[Ee][-+]?\d+)?", text)
    return [float(item) for item in numbers]


def _target_values_from_command(command: Any, *, group: int) -> List[float]:
    text = str(command or "").strip()
    prefix = f"SENCO{int(group)},"
    if not text.upper().startswith(prefix):
        raise ValueError(f"Candidate command for SENCO{group} is malformed: {text!r}")
    parts = text.split(",", 3)
    if len(parts) != 4:
        raise ValueError(f"Candidate command for SENCO{group} is malformed: {text!r}")
    values = _parse_values(parts[3])
    if len(values) != 4:
        raise ValueError(f"SENCO{group} candidate must contain exactly 4 values, got {values!r}")
    return values


def _read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: List[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(str(key))
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _load_candidate_rows(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for row in _read_csv(path):
        try:
            group = _parse_channel(row.get("channel"))
            target_values = _target_values_from_command(row.get("candidate_command"), group=group)
        except Exception as exc:
            item = dict(row)
            item["_load_status"] = "invalid"
            item["_load_error"] = str(exc)
            rows.append(item)
            continue
        item = dict(row)
        item["_load_status"] = "valid"
        item["_senco_group"] = group
        item["_target_values"] = target_values
        item["_device_id"] = _device_id(item.get("device_id"))
        rows.append(item)
    return rows


def _select_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    selected_device_ids: Sequence[str],
    selected_channels: Sequence[int],
    write_all_recommended: bool,
) -> List[Dict[str, Any]]:
    wanted_ids = {_device_id(item) for item in selected_device_ids if str(item or "").strip()}
    wanted_channels = {int(item) for item in selected_channels}
    selected: List[Dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        if item.get("_load_status") != "valid":
            continue
        if write_all_recommended and str(item.get("decision") or "").strip() != "write_recommended":
            continue
        if wanted_ids and item.get("_device_id") not in wanted_ids:
            continue
        if wanted_channels and int(item.get("_senco_group")) not in wanted_channels:
            continue
        if not write_all_recommended and not wanted_ids:
            continue
        selected.append(item)
    return selected


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Blocked V1.5 SENCO7/SENCO8 candidate writer compatibility guard."
    )
    parser.add_argument("--config", required=True, help="Accepted for compatibility; not opened.")
    parser.add_argument("--review-csv", required=True, help="temperature_channel_write_review_zh.csv.")
    parser.add_argument("--output-dir", required=True, help="Output directory for blocked evidence.")
    parser.add_argument("--device-id", action="append", default=[])
    parser.add_argument("--channel", action="append", type=int, choices=SUPPORTED_CHANNELS, default=[])
    parser.add_argument("--write-all-recommended", action="store_true")
    parser.add_argument("--enable-senco78-write", action="store_true")
    parser.add_argument("--operator-confirmation", default="", help=f"Legacy confirmation text: {CONFIRMATION_TEXT!r}.")
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
    parser.add_argument("--config-ack-retry-count", type=int, default=0)
    parser.add_argument("--config-ack-retry-delay-s", type=float, default=1.0)
    parser.add_argument("--compare-atol", type=float, default=5e-5)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    review_rows = _load_candidate_rows(Path(args.review_csv))
    selected_rows = _select_rows(
        review_rows,
        selected_device_ids=args.device_id,
        selected_channels=args.channel,
        write_all_recommended=bool(args.write_all_recommended),
    )
    events: List[Dict[str, Any]] = []
    for row in selected_rows:
        events.append(
            {
                "timestamp": datetime.now().isoformat(timespec="seconds"),
                "device_id": row.get("_device_id", ""),
                "review_csv_port": row.get("port", ""),
                "channel": f"SENCO{int(row['_senco_group'])}",
                "status": "blocked_no_write",
                "reason": BLOCK_REASON,
                "candidate_values": json.dumps(row.get("_target_values", []), ensure_ascii=False),
                "opens_com_ports": False,
                "writes_coefficients": False,
            }
        )

    events_csv = output_dir / "senco78_candidate_write_events.csv"
    _write_csv(events_csv, events)
    summary = {
        "tool": "run_v1_5_temperature_senco78_candidate_controlled_write",
        "ok": False,
        "blocked": True,
        "production_state": "blocked",
        "reason": BLOCK_REASON,
        "policy": "V1.5 production keeps SENCO7/SENCO8 neutral; candidate temperature writes are not an approved production path.",
        "review_csv": str(Path(args.review_csv).resolve()),
        "events_csv": str(events_csv),
        "opens_com_ports": False,
        "writes_coefficients": False,
        "controls_water_or_gas_routes": False,
        "controls_pressure": False,
        "selected_device_ids": sorted({str(row.get("_device_id", "")) for row in selected_rows if row.get("_device_id")}),
        "selected_count": len(selected_rows),
        "confirmation_text_seen": str(args.operator_confirmation).strip() == CONFIRMATION_TEXT,
    }
    (output_dir / "senco78_candidate_write_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(
        "SENCO7/8 candidate temperature writes are blocked by V1.5 neutral-temperature policy.",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
