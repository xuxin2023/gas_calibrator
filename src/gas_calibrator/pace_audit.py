"""PACE runtime audit helpers for V1 engineering no-write runs."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Mapping, Optional


PACE_RAW_TAP_FIELDS = [
    "wall_ts",
    "monotonic_ts",
    "run_id",
    "device_label",
    "port",
    "direction",
    "raw_bytes_hex",
    "raw_text_decoded",
    "decoded_command",
    "command_category",
    "is_state_changing_command",
    "thread_name",
    "workflow_stage",
    "python_call_stack_top10",
    "linked_io_log_sequence_id",
]


def cfg_get(mapping: Mapping[str, Any], path: str, default: Any = None) -> Any:
    current: Any = mapping
    for part in str(path or "").split("."):
        if not isinstance(current, Mapping):
            return default
        if part not in current:
            return default
        current = current[part]
    return current


def safe_port_label(port: Any) -> str:
    text = str(port or "").strip().upper()
    return re.sub(r"[^A-Z0-9_.-]+", "_", text).strip("_") or "UNKNOWN"


def decode_raw_bytes(raw_bytes: Any) -> str:
    if raw_bytes is None:
        return ""
    if isinstance(raw_bytes, str):
        return raw_bytes
    try:
        return bytes(raw_bytes).decode("ascii", errors="ignore")
    except Exception:
        try:
            return str(raw_bytes)
        except Exception:
            return ""


def normalize_pace_command(raw_text: Any) -> str:
    text = decode_raw_bytes(raw_text)
    return text.replace("\r", "").replace("\n", "").strip()


def classify_pace_command(raw_text: Any) -> str:
    command = normalize_pace_command(raw_text).upper()
    if not command:
        return "UNKNOWN"
    if "VENT" in command:
        return "VENT"
    if "OUTP" in command:
        return "OUTP"
    if "ISOL" in command:
        return "ISOL"
    if "MODE" in command:
        return "MODE"
    if "RANG" in command:
        return "RANGE"
    if "SOUR:PRES" in command or "SETPOINT" in command or "SETP" in command:
        return "SETPOINT"
    if "INL" in command or "IN_LIMIT" in command:
        return "IN_LIMITS"
    if "SYST" in command:
        return "SYST"
    if "STAT" in command or "COND" in command or "EVEN" in command:
        return "STATUS"
    if "MEAS:PRES" in command or "SENS:PRES" in command or "PRES?" in command:
        return "PRESSURE_QUERY"
    return "UNKNOWN"


def is_pace_query(raw_text: Any) -> bool:
    command = normalize_pace_command(raw_text)
    return "?" in command


def is_vent1_command(raw_text: Any) -> bool:
    command = normalize_pace_command(raw_text).upper()
    if "VENT" not in command:
        return False
    return re.search(r"VENT\s+(\+?1(?:\.0*)?)\s*$", command) is not None


def is_state_changing_pace_command(raw_text: Any) -> bool:
    if is_pace_query(raw_text):
        return False
    category = classify_pace_command(raw_text)
    if category in {"PRESSURE_QUERY", "STATUS"}:
        return False
    return category in {
        "VENT",
        "OUTP",
        "ISOL",
        "MODE",
        "RANGE",
        "SETPOINT",
        "IN_LIMITS",
        "SYST",
        "UNKNOWN",
    }


def is_allowed_analyzer_gate_pace_write(raw_text: Any) -> bool:
    return is_pace_query(raw_text) or is_vent1_command(raw_text)


class PressureControllerComLockExists(RuntimeError):
    def __init__(self, lock_path: Path, existing: str):
        super().__init__(f"pressure controller COM lock exists: {lock_path}")
        self.lock_path = lock_path
        self.existing = existing


@dataclass
class PressureControllerComLock:
    path: Path
    port: str
    released: bool = False

    def close(self) -> None:
        if self.released:
            return
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass
        finally:
            self.released = True


def _git_value(cwd: Path, *args: str) -> str:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=str(cwd),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=2,
            check=False,
        )
    except Exception:
        return ""
    if result.returncode != 0:
        return ""
    return result.stdout.strip()


def _lock_payload(
    *,
    cfg: Mapping[str, Any],
    run_id: str,
    config_path: Any,
    port: str,
) -> Dict[str, Any]:
    cwd = Path.cwd()
    return {
        "pid": os.getpid(),
        "process_command_line": " ".join([sys.executable, *sys.argv]),
        "cwd": str(cwd),
        "run_id": str(run_id or ""),
        "config_path": str(config_path or ""),
        "branch": _git_value(cwd, "branch", "--show-current"),
        "head": _git_value(cwd, "rev-parse", "HEAD"),
        "created_at": datetime.now().isoformat(timespec="milliseconds"),
        "port": str(port or ""),
    }


def pressure_com_lock_enabled(cfg: Mapping[str, Any]) -> bool:
    return bool(cfg_get(cfg, "workflow.pressure.com_lock.enabled", False))


def acquire_pressure_controller_com_lock(
    cfg: Mapping[str, Any],
    *,
    run_id: str = "",
    config_path: Any = "",
) -> Optional[PressureControllerComLock]:
    if not pressure_com_lock_enabled(cfg):
        return None
    device = str(cfg_get(cfg, "workflow.pressure.com_lock.device", "pressure_controller") or "")
    if device.strip().lower() != "pressure_controller":
        return None
    port = str(cfg_get(cfg, "devices.pressure_controller.port", "") or "").strip()
    if not port:
        return None
    output_dir = Path(str(cfg_get(cfg, "paths.output_dir", "logs") or "logs"))
    lock_dir_cfg = cfg_get(cfg, "workflow.pressure.com_lock.lock_dir", None)
    lock_dir = Path(str(lock_dir_cfg)) if lock_dir_cfg else output_dir / "_runtime_locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    path = lock_dir / f"pace_{safe_port_label(port)}.lock"
    payload = _lock_payload(cfg=cfg, run_id=run_id, config_path=config_path, port=port)
    try:
        fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        try:
            existing = path.read_text(encoding="utf-8")
        except Exception as exc:
            existing = f"<unable to read existing lock: {exc}>"
        raise PressureControllerComLockExists(path, existing)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
    except Exception:
        try:
            path.unlink()
        except Exception:
            pass
        raise
    return PressureControllerComLock(path=path, port=port)
