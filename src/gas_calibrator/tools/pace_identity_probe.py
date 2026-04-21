"""Read-only PACE identity probe for formal adapter auditing."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

from ..config import load_config
from ..devices import Pace5000
from ..logging_utils import RunLogger


DEFAULT_COMMANDS: Sequence[str] = (
    "*IDN?",
    ":SYST:VERS?",
    ":INST:MOD?",
    ":SYST:ECHO?",
    ":INST:SN?",
    ":INST:VERS?",
    ":INST:SENS?",
    ":INST:CAT:ALL?",
    ":UNIT:PRES?",
    ":OUTP:STAT?",
    ":SOUR:PRES?",
    ":SOUR:PRES:RANG?",
    ":SENS:PRES:CONT?",
    ":SENS:PRES:INL?",
    ":SYST:ERR?",
)


def _response_payload(text: Any) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    parts = raw.split(None, 1)
    return parts[1].strip() if len(parts) > 1 else raw


def _parse_first_int(text: Any) -> int | None:
    payload = _response_payload(text)
    token = payload.split(",", 1)[0].strip()
    try:
        return int(float(token))
    except Exception:
        return None


def _looks_like_undefined_header(text: Any) -> bool:
    code = _parse_first_int(text)
    payload = _response_payload(text).lower()
    return code == -113 or "undefined header" in payload


def _determine_profile(rows: Sequence[Dict[str, Any]]) -> str:
    by_command = {str(row.get("command") or "").strip().upper(): row for row in rows}
    idn_text = str(by_command.get("*IDN?", {}).get("response") or "")
    mod_text = str(by_command.get(":INST:MOD?", {}).get("response") or "")
    echo_text = str(by_command.get(":SYST:ECHO?", {}).get("response") or "")
    if "GE DRUCK" in idn_text.upper() or "PACE5000 USER INTERFACE" in idn_text.upper():
        return Pace5000.PROFILE_OLD_PACE5000
    if _response_payload(mod_text).strip().strip("\"'").upper() == Pace5000.PROFILE_PACE5000E:
        return Pace5000.PROFILE_PACE5000E
    if _looks_like_undefined_header(mod_text) or _looks_like_undefined_header(echo_text):
        return Pace5000.PROFILE_OLD_PACE5000
    return Pace5000.PROFILE_UNKNOWN


def run_probe(
    *,
    config_path: str,
    output_dir: str,
    commands: Sequence[str] = DEFAULT_COMMANDS,
    clear_before_probe: bool = False,
) -> Dict[str, Any]:
    cfg = load_config(config_path)
    pcfg = dict(cfg.get("devices", {}).get("pressure_controller", {}) or {})
    logger = RunLogger(Path(output_dir), run_id=f"pace_identity_probe_{datetime.now().strftime('%Y%m%d_%H%M%S')}", cfg=cfg)
    pace = Pace5000(
        pcfg["port"],
        int(pcfg.get("baud", 9600)),
        timeout=float(pcfg.get("timeout", 1.0)),
        line_ending=pcfg.get("line_ending"),
        query_line_endings=pcfg.get("query_line_endings"),
        pressure_queries=pcfg.get("pressure_queries"),
        io_logger=logger,
    )
    rows: List[Dict[str, Any]] = []
    probe_commands = list(commands)
    if clear_before_probe:
        probe_commands = ["*CLS", *probe_commands]
    try:
        pace.open()
        rows = pace.probe_identity(probe_commands)
    finally:
        try:
            pace.close()
        except Exception:
            pass
        logger.close()

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = logger.run_dir / f"pace_identity_probe_{stamp}.csv"
    json_path = logger.run_dir / f"pace_identity_probe_{stamp}.json"
    with csv_path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["command", "response", "duration_ms", "error"],
        )
        writer.writeheader()
        writer.writerows(rows)

    profile = _determine_profile(rows)
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "config_path": str(Path(config_path).resolve()),
        "run_dir": str(logger.run_dir),
        "io_path": str(logger.io_path),
        "csv_path": str(csv_path),
        "json_path": str(json_path),
        "profile": profile,
        "commands": list(probe_commands),
        "state_changing_clear_executed": bool(clear_before_probe),
        "rows": rows,
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only PACE identity probe")
    parser.add_argument("--config", default="configs/default_config.json")
    parser.add_argument("--output-dir", default="audit/real_pace_controller_acceptance")
    parser.add_argument("--clear-before-probe", action="store_true")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    result = run_probe(
        config_path=str(args.config),
        output_dir=str(args.output_dir),
        clear_before_probe=bool(args.clear_before_probe),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
