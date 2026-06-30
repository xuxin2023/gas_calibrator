"""Overlay controlled write/readback events onto a V1.5 GETCO snapshot.

This utility is offline-only.  It never opens COM ports or controls any
calibration route.  It exists to keep the evidence chain honest when an initial
GETCO snapshot is followed by controlled neutralization or repair events.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence


ACCEPTED_EVENT_STATUSES = {
    "already_neutral",
    "written_readback_verified",
    "readback_verified",
}


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _normal_device_id(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.startswith("GA"):
        text = text[2:]
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _safe_bool(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "pass", "ok"}


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "None", "null"):
        return None
    try:
        number = float(value)
    except Exception:
        return None
    if number != number or number in (float("inf"), float("-inf")):
        return None
    return number


def _parse_values(value: Any) -> list[float]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        out: list[float] = []
        for item in value:
            number = _safe_float(item)
            if number is None:
                return []
            out.append(float(number))
        return out
    text = str(value or "").strip()
    if not text:
        return []
    try:
        decoded = json.loads(text)
    except Exception:
        decoded = None
    if isinstance(decoded, (list, tuple)):
        return _parse_values(decoded)
    numbers = re.findall(r"[-+]?\d+(?:\.\d+)?(?:[Ee][-+]?\d+)?", text)
    return [float(item) for item in numbers]


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(str(key))
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _parsed_values(values: Sequence[float]) -> dict[str, float]:
    return {f"C{idx}": float(value) for idx, value in enumerate(values)}


def _infer_group_from_row(row: Mapping[str, Any], path: Path) -> Optional[int]:
    for key in row:
        match = re.match(r"^(?:final|target|old)_senco(?P<group>\d+)", str(key or ""), re.I)
        if match:
            return int(match.group("group"))
    match = re.search(r"senco(?P<group>\d+)", path.name, re.I)
    if match:
        return int(match.group("group"))
    return None


def _values_for_group(row: Mapping[str, Any], group: int) -> tuple[list[float], str]:
    keys = (
        f"final_senco{group}",
        f"target_senco{group}_values",
        f"target_senco{group}",
        f"readback_senco{group}",
        f"current_senco{group}",
    )
    for key in keys:
        if key in row:
            values = _parse_values(row.get(key))
            if values:
                return values, key
    return [], ""


def _event_is_accepted(row: Mapping[str, Any], values: Sequence[float]) -> tuple[bool, str]:
    status = str(row.get("status") or "").strip()
    if status in ACCEPTED_EVENT_STATUSES and values:
        return True, f"accepted_status:{status}"
    if _safe_bool(row.get("readback_verified")) and values:
        return True, "accepted_readback_verified"
    return False, f"ignored_status:{status or 'missing'}"


def _apply_event_rows(
    snapshot: dict[str, Any],
    event_csvs: Sequence[Path],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    current = deepcopy(snapshot)
    audit_rows: list[dict[str, Any]] = []
    for event_path in event_csvs:
        rows = _read_csv(event_path)
        for index, row in enumerate(rows, start=1):
            group = _infer_group_from_row(row, event_path)
            device_id = _normal_device_id(
                row.get("device_id")
                or row.get("analyzer_device_id")
                or row.get("runtime_device_id")
                or row.get("identity_after")
                or row.get("identity_before")
            )
            values, value_source = _values_for_group(row, group or -1) if group else ([], "")
            accepted, reason = _event_is_accepted(row, values)
            audit = {
                "event_csv": str(event_path),
                "row_index": index,
                "device_id": device_id,
                "group": group or "",
                "status": row.get("status", ""),
                "readback_verified": row.get("readback_verified", ""),
                "value_source": value_source,
                "values": json.dumps(values, ensure_ascii=False),
                "overlay_applied": bool(accepted and group and device_id),
                "reason": reason,
            }
            if accepted and group and device_id:
                device = current.setdefault(device_id, {})
                if isinstance(device, dict):
                    device[f"GETCO{group}_before"] = list(values)
                    device[f"GETCO{group}_before_parsed"] = _parsed_values(values)
                    device[f"GETCO{group}_before_command"] = (
                        f"overlay_from:{event_path.name}:{value_source}"
                    )
                    device[f"GETCO{group}_overlay_source"] = str(event_path)
                    device[f"GETCO{group}_overlay_status"] = str(row.get("status") or "")
                    device[f"GETCO{group}_overlay_applied_at"] = _now()
            audit_rows.append(audit)
    return current, audit_rows


def _snapshot_rows(snapshot: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for device_id in sorted(snapshot):
        item = snapshot.get(device_id)
        if not isinstance(item, Mapping):
            continue
        for group in range(1, 10):
            values = item.get(f"GETCO{group}_before")
            if values in (None, ""):
                continue
            rows.append(
                {
                    "device_id": device_id,
                    "analyzer_prefix": item.get("analyzer_prefix", ""),
                    "port": item.get("port", ""),
                    "getco_group": group,
                    "values_json": json.dumps(values, ensure_ascii=False),
                    "command": item.get(f"GETCO{group}_before_command", ""),
                    "overlay_source": item.get(f"GETCO{group}_overlay_source", ""),
                    "overlay_status": item.get(f"GETCO{group}_overlay_status", ""),
                }
            )
    return rows


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a final-current V1.5 GETCO snapshot by overlaying controlled event readbacks."
    )
    parser.add_argument("--base-snapshot-json", required=True)
    parser.add_argument("--event-csv", action="append", default=[])
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    base_path = Path(args.base_snapshot_json)
    output_dir = Path(args.output_dir)
    event_csvs = [Path(item) for item in args.event_csv or []]
    try:
        base_snapshot = json.loads(base_path.read_text(encoding="utf-8"))
        current_snapshot, audit_rows = _apply_event_rows(base_snapshot, event_csvs)
        output_dir.mkdir(parents=True, exist_ok=True)
        snapshot_path = output_dir / "current_component_coefficients_snapshot.json"
        _write_json(snapshot_path, current_snapshot)
        _write_csv(output_dir / "current_component_snapshot_rows.csv", _snapshot_rows(current_snapshot))
        _write_csv(output_dir / "component_snapshot_overlay_audit.csv", audit_rows)
        summary = [
            "# V1.5 最终当前 GETCO 快照合成",
            "",
            f"- base_snapshot: `{base_path}`",
            f"- event_csv_count: {len(event_csvs)}",
            f"- overlay_applied_rows: {sum(1 for row in audit_rows if row.get('overlay_applied'))}",
            f"- generated_at: {_now()}",
            "",
            "物理意义：该快照表示主校准拟合前后评审应采用的当前输出层状态，避免把早期旧快照中的 S5/S6 当成仍然有效的最终显示层。",
        ]
        (output_dir / "component_snapshot_overlay_summary_zh.md").write_text(
            "\n".join(summary) + "\n",
            encoding="utf-8",
        )
    except Exception as exc:
        print(f"V1.5 component snapshot overlay export failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(json.dumps({"snapshot": str(snapshot_path.resolve())}, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
