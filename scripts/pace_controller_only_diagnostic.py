from __future__ import annotations

import argparse
import csv
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

from gas_calibrator.devices.pace5000 import Pace5000


CSV_FIELDS = [
    "timestamp",
    "pressure_hpa",
    "outp_state",
    "isol_state",
    "mode",
    "vent_status",
    "vent_elapsed_time_s",
    "after_vent_valve_state",
    "popup_state",
    "orpv_state",
    "pupv_state",
    "oper_cond",
    "oper_pres_cond",
]


def _timestamp() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _snapshot_row(status: Dict[str, Any], *, timestamp: Optional[str] = None) -> Dict[str, Any]:
    return {
        "timestamp": timestamp or _timestamp(),
        "pressure_hpa": status.get("pressure_hpa", ""),
        "outp_state": status.get("output_state", ""),
        "isol_state": status.get("isolation_state", ""),
        "mode": status.get("output_mode", ""),
        "vent_status": status.get("vent_status", ""),
        "vent_elapsed_time_s": status.get("vent_elapsed_time_s", ""),
        # AFT:VVAL is the configured post-vent valve policy, not proof of the
        # current instantaneous physical valve position.
        "after_vent_valve_state": status.get("vent_after_valve_state", ""),
        "popup_state": status.get("vent_popup_state", ""),
        "orpv_state": status.get("vent_orpv_state", ""),
        "pupv_state": status.get("vent_pupv_state", ""),
        "oper_cond": status.get("oper_condition", ""),
        "oper_pres_cond": status.get("oper_pressure_condition", ""),
    }


def _sanitize_vent_policy(pace: Pace5000) -> Dict[str, Any]:
    before = pace.diagnostic_status()
    pace.set_vent_after_valve_open(False)
    after = pace.diagnostic_status()
    return {
        "performed": True,
        "command": "VENT:AFT:VVAL:STAT CLOSed",
        "before": _snapshot_row(before),
        "after": _snapshot_row(after),
    }


def _write_csv(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def run_controller_only_diagnostic(
    *,
    port: str,
    baudrate: int = 9600,
    timeout: float = 1.0,
    samples: int = 120,
    interval_s: float = 0.5,
    output_dir: Path | str,
    allow_write_sanitize: bool = False,
    pace_factory: Optional[Callable[..., Pace5000]] = None,
) -> Dict[str, Any]:
    output_path = Path(output_dir).resolve()
    output_path.mkdir(parents=True, exist_ok=True)
    factory = pace_factory or Pace5000
    pace = factory(port, baudrate=baudrate, timeout=timeout)
    rows: List[Dict[str, Any]] = []
    sanitize_summary: Dict[str, Any] = {"performed": False}
    pace.open()
    try:
        if allow_write_sanitize:
            sanitize_summary = _sanitize_vent_policy(pace)
        for index in range(max(1, int(samples))):
            rows.append(_snapshot_row(pace.diagnostic_status()))
            if index + 1 < max(1, int(samples)):
                time.sleep(max(0.0, float(interval_s)))
    finally:
        pace.close()

    csv_path = output_path / "pace_controller_only_diagnostic.csv"
    json_path = output_path / "pace_controller_only_diagnostic.json"
    _write_csv(csv_path, rows)
    summary = {
        "port": port,
        "baudrate": baudrate,
        "timeout": timeout,
        "samples": max(1, int(samples)),
        "interval_s": max(0.0, float(interval_s)),
        "allow_write_sanitize": bool(allow_write_sanitize),
        "sanitize_summary": sanitize_summary,
        "csv_path": str(csv_path),
        "json_path": str(json_path),
        "rows": rows,
        "notes": [
            "read-only by default",
            "allow-write-sanitize only sends VENT:AFT:VVAL:STAT CLOSed",
            "no setpoint, output, or vent control actions are performed",
        ],
    }
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="PACE controller-only diagnostic poller.")
    parser.add_argument("--port", required=True, help="PACE serial port, for example COM7.")
    parser.add_argument("--baudrate", type=int, default=9600)
    parser.add_argument("--timeout", type=float, default=1.0)
    parser.add_argument("--samples", type=int, default=120, help="Number of read-only snapshots to collect.")
    parser.add_argument("--interval-s", type=float, default=0.5, help="Polling interval in seconds.")
    parser.add_argument("--output-dir", required=True, help="Directory for CSV/JSON output.")
    parser.add_argument(
        "--allow-write-sanitize",
        action="store_true",
        help="Allow exactly one config write: VENT:AFT:VVAL:STAT CLOSed.",
    )
    args = parser.parse_args(argv)
    summary = run_controller_only_diagnostic(
        port=args.port,
        baudrate=args.baudrate,
        timeout=args.timeout,
        samples=args.samples,
        interval_s=args.interval_s,
        output_dir=args.output_dir,
        allow_write_sanitize=args.allow_write_sanitize,
    )
    print(f"saved csv: {summary['csv_path']}")
    print(f"saved json: {summary['json_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
