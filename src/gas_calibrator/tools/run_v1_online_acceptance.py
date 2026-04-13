"""Guarded V1 online acceptance helper for real-device abnormal-recovery evidence.

Default behavior is dry-run only. Real device access requires both:
1. CLI flag: --real-device
2. ENV gate: ALLOW_REAL_DEVICE_WRITE=1

This entrypoint is part of the V1 runtime boundary and must stay independent
from V2 runtime code.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence

from ..config import (
    V1_CO2_ONLY_H2O_NOT_SUPPORTED_MESSAGE,
    load_config,
    require_v1_h2o_zero_span_supported,
)
from ..devices import GasAnalyzer
from .run_v1_corrected_autodelivery import write_senco_groups_with_full_verification


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "audit" / "v1_calibration_acceptance_online"
REAL_DEVICE_ENV = "ALLOW_REAL_DEVICE_WRITE"
CO2_ONLY_GROUPS = {1, 3}
REQUIRED_RUN_FIELDS = [
    "run_id",
    "session_id",
    "device_id",
    "start_ts",
    "end_ts",
    "mode_before",
    "mode_after",
    "mode_exit_attempted",
    "mode_exit_confirmed",
    "coeff_before",
    "coeff_target",
    "coeff_readback",
    "rollback_attempted",
    "rollback_confirmed",
    "write_status",
    "verify_status",
    "rollback_status",
    "unsafe",
    "failure_reason",
]
REQUIRED_PROTOCOL_FIELDS = [
    "ts",
    "stage",
    "action",
    "raw_command",
    "raw_response",
    "error",
]


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _safe_bool(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def _head_commit() -> str:
    proc = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if proc.returncode == 0:
        return proc.stdout.strip()
    return "UNKNOWN"


def _log(message: str) -> None:
    print(message, flush=True)


def _parse_group_list(raw: str | None) -> List[int]:
    groups: List[int] = []
    for part in str(raw or "").split(","):
        text = part.strip()
        if not text:
            continue
        groups.append(int(text))
    return groups or [1]


def _normalize_groups(groups: Sequence[int]) -> List[int]:
    normalized = sorted({int(group) for group in groups})
    unsupported = [group for group in normalized if int(group) not in CO2_ONLY_GROUPS]
    if unsupported:
        raise RuntimeError(
            f"run_v1_online_acceptance: {V1_CO2_ONLY_H2O_NOT_SUPPORTED_MESSAGE} "
            f"Unsupported groups requested: {unsupported}. Supported CO2 groups: {sorted(CO2_ONLY_GROUPS)}."
        )
    return normalized or [1]


def _resolve_analyzer_cfg(
    cfg: Mapping[str, Any],
    *,
    analyzer: Optional[str],
) -> Dict[str, Any]:
    devices_cfg = cfg.get("devices", {}) if isinstance(cfg, Mapping) else {}
    gas_cfg = devices_cfg.get("gas_analyzers", []) if isinstance(devices_cfg, Mapping) else []
    target = str(analyzer or "").strip().upper()
    if isinstance(gas_cfg, list) and gas_cfg:
        for idx, item in enumerate(gas_cfg, start=1):
            if not isinstance(item, Mapping) or not item.get("enabled", True):
                continue
            name = str(item.get("name") or f"GA{idx:02d}").upper()
            device_id = str(item.get("device_id", "") or "").upper()
            if not target or target in {name, device_id, str(idx)}:
                return dict(item)
    single_cfg = devices_cfg.get("gas_analyzer", {}) if isinstance(devices_cfg, Mapping) else {}
    if isinstance(single_cfg, Mapping) and single_cfg.get("enabled", False):
        return dict(single_cfg)
    raise RuntimeError(f"Analyzer selection not found: {analyzer}")


def _load_target_groups(path: str | Path) -> Dict[int, List[float]]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    out: Dict[int, List[float]] = {}
    for key, values in dict(raw).items():
        group = int(key)
        if group not in CO2_ONLY_GROUPS:
            raise RuntimeError(
                f"run_v1_online_acceptance: {V1_CO2_ONLY_H2O_NOT_SUPPORTED_MESSAGE} "
                f"Target payload includes unsupported group {group}."
            )
        if isinstance(values, Mapping):
            ordered = [
                float(value)
                for _name, value in sorted(values.items(), key=lambda item: int(str(item[0]).lstrip("C")))
            ]
        else:
            ordered = [float(value) for value in list(values)]
        out[group] = ordered
    return out


def _ensure_co2_only_request(
    coeff_cfg: Mapping[str, Any] | None,
    *,
    gas_type: str,
    groups: Sequence[int],
) -> Dict[str, Any]:
    normalized_gas_type = str(gas_type or "").strip().lower()
    if normalized_gas_type != "co2":
        raise RuntimeError(
            f"run_v1_online_acceptance: {V1_CO2_ONLY_H2O_NOT_SUPPORTED_MESSAGE} "
            f"Requested gas_type={normalized_gas_type!r}."
        )
    capability = require_v1_h2o_zero_span_supported(
        coeff_cfg,
        context="run_v1_online_acceptance",
    )
    _normalize_groups(groups)
    return {
        **capability,
        "supported_groups": sorted(CO2_ONLY_GROUPS),
        "selected_groups": sorted(int(group) for group in groups),
        "gas_type": "co2",
    }


class ProtocolRecordingAnalyzer:
    def __init__(self, analyzer: Any, *, session_id: str) -> None:
        self._analyzer = analyzer
        self._session_id = session_id
        self.protocol_rows: List[Dict[str, Any]] = []

    def _record(
        self,
        *,
        stage: str,
        action: str,
        raw_command: str,
        raw_response: Any = "",
        error: str = "",
    ) -> None:
        self.protocol_rows.append(
            {
                "ts": _now_iso(),
                "session_id": self._session_id,
                "stage": stage,
                "action": action,
                "raw_command": str(raw_command),
                "raw_response": raw_response if isinstance(raw_response, str) else _json_text(raw_response),
                "error": str(error or ""),
            }
        )

    def read_current_mode_snapshot(self):
        try:
            response = self._analyzer.read_current_mode_snapshot()
        except Exception as exc:
            self._record(
                stage="baseline-mode-snapshot",
                action="read_current_mode_snapshot",
                raw_command="READ_CURRENT_MODE_SNAPSHOT",
                error=str(exc),
            )
            raise
        self._record(
            stage="baseline-mode-snapshot",
            action="read_current_mode_snapshot",
            raw_command="READ_CURRENT_MODE_SNAPSHOT",
            raw_response=response,
        )
        return response

    def set_mode_with_ack(self, mode: int, *, require_ack: bool = True) -> bool:
        try:
            response = bool(self._analyzer.set_mode_with_ack(int(mode), require_ack=require_ack))
        except Exception as exc:
            self._record(
                stage="mode-switch",
                action="set_mode_with_ack",
                raw_command=f"MODE,{int(mode)}",
                error=str(exc),
            )
            raise
        self._record(
            stage="mode-switch",
            action="set_mode_with_ack",
            raw_command=f"MODE,{int(mode)}",
            raw_response={"ack": response, "require_ack": bool(require_ack)},
        )
        return response

    def set_senco(self, group: int, *coeffs: Any) -> bool:
        values = list(coeffs[0]) if len(coeffs) == 1 and isinstance(coeffs[0], (list, tuple)) else list(coeffs)
        payload = [float(value) for value in values]
        try:
            response = bool(self._analyzer.set_senco(int(group), *payload))
        except Exception as exc:
            self._record(
                stage="write-coefficients",
                action="set_senco",
                raw_command=f"SENCO{int(group)}",
                raw_response=payload,
                error=str(exc),
            )
            raise
        self._record(
            stage="write-coefficients",
            action="set_senco",
            raw_command=f"SENCO{int(group)}",
            raw_response={"ack": response, "coefficients": payload},
        )
        return response

    def read_coefficient_group(self, group: int):
        try:
            response = self._analyzer.read_coefficient_group(int(group))
        except Exception as exc:
            self._record(
                stage="getco-readback",
                action="read_coefficient_group",
                raw_command=f"GETCO{int(group)}",
                error=str(exc),
            )
            raise
        self._record(
            stage="getco-readback",
            action="read_coefficient_group",
            raw_command=f"GETCO{int(group)}",
            raw_response=response,
        )
        return response

    def __getattr__(self, name: str) -> Any:
        return getattr(self._analyzer, name)


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(dict(row), ensure_ascii=False, default=str) + "\n")


def build_online_acceptance_checklist(*, generated_at: str, head: str) -> str:
    lines = [
        "# Online Acceptance Checklist",
        "",
        f"- generated_at: {generated_at}",
        f"- head: `{head}`",
        f"- dual_gate_required: CLI `--real-device` + ENV `{REAL_DEVICE_ENV}=1`",
        f"- capability_boundary: {V1_CO2_ONLY_H2O_NOT_SUPPORTED_MESSAGE}",
        "",
        "## Startup Checks",
        "",
        "- Confirm this tool is running on the intended fixed HEAD before any real-device action.",
        "- Confirm V1 H2O zero/span is NOT_SUPPORTED and do not request H2O groups.",
        "- Confirm default runtime safety remains intact: postrun real write stays disabled unless explicitly opted in elsewhere.",
        "- Confirm the selected analyzer/device ID matches the bench device you intend to observe.",
        "- Confirm the planned coefficient groups are CO2-only (`1`, `3`) and no H2O group is requested.",
        "",
        "## Dual-Gate Confirmation",
        "",
        "- Gate 1: pass CLI flag `--real-device` only for an intentional bench session.",
        f"- Gate 2: set environment variable `{REAL_DEVICE_ENV}=1` in the same shell/session.",
        "- If either gate is missing, this tool must stay in dry-run mode and must not instantiate or write any device.",
        "",
        "## Real-Device Steps",
        "",
        "- Run one dry-run first and confirm only templates/checklists are produced.",
        "- For an authorized bench session, capture baseline mode snapshot before any write.",
        "- Enter calibration mode (`MODE=2`).",
        "- Write the planned CO2 coefficient group(s).",
        "- Perform immediate `GETCO` readback and compare against target values.",
        "- If readback mismatches or any protocol step fails, confirm rollback was attempted and review rollback readback.",
        "- Confirm final mode-restore attempt back to normal mode and collect final mode snapshot.",
        "",
        "## Manual Observations",
        "",
        "- Record visible bench symptoms during mode switch, write, readback, rollback, and exit.",
        "- Record whether the analyzer front panel / service UI / serial trace indicates normal mode after the run.",
        "- Record any protocol noise, delayed ACK, empty readback, parse anomaly, or operator intervention.",
        "",
        "## Abort Conditions",
        "",
        "- Abort immediately if the selected target is not a CO2-only group or if any H2O zero/span request appears.",
        "- Abort immediately if either real-device gate is missing or ambiguous.",
        "- Abort immediately if the baseline mode cannot be read, if calibration mode cannot be entered, or if final mode cannot be confirmed after restore attempt.",
        "- Abort immediately if rollback is attempted but cannot be confirmed.",
        "",
        "## When The Result Must Stay ONLINE_EVIDENCE_REQUIRED",
        "",
        "- No real-device `online_run_*.json` and `online_protocol_*.jsonl` have been captured yet.",
        "- A run stayed dry-run only, even if all offline checks passed.",
        "- Final mode exit was attempted but not confirmed (`mode_exit_confirmed=false`).",
        "- Rollback was attempted but not confirmed (`rollback_confirmed=false`).",
        "- The run ended with `unsafe=true`, `FAILED`, or any missing raw protocol evidence.",
        "",
        "## Reminder",
        "",
        "- Offline evidence does not replace real acceptance evidence; online abnormal-recovery proof remains required until real logs are captured and reviewed.",
        "",
    ]
    return "\n".join(lines)


def build_online_run_template(*, generated_at: str, head: str) -> Dict[str, Any]:
    return {
        "generated_at": generated_at,
        "head": head,
        "status": "ONLINE_EVIDENCE_REQUIRED",
        "mode": "dry_run",
        "evidence_source": "real_device_when_authorized",
        "dual_gate_required": {
            "cli_flag": "--real-device",
            "environment_variable": f"{REAL_DEVICE_ENV}=1",
        },
        "capability_boundary": {
            "co2_zero": "PASS",
            "co2_span": "PASS",
            "h2o_zero": "NOT_SUPPORTED",
            "h2o_span": "NOT_SUPPORTED",
            "message": V1_CO2_ONLY_H2O_NOT_SUPPORTED_MESSAGE,
            "supported_groups": sorted(CO2_ONLY_GROUPS),
        },
        "required_run_fields": list(REQUIRED_RUN_FIELDS),
        "required_protocol_fields": list(REQUIRED_PROTOCOL_FIELDS),
    }


def build_online_protocol_log_schema(*, generated_at: str, head: str) -> str:
    lines = [
        "# Online Protocol Log Schema",
        "",
        f"- generated_at: {generated_at}",
        f"- head: `{head}`",
        "",
        "## JSONL Fields",
        "",
    ]
    for field_name in REQUIRED_PROTOCOL_FIELDS:
        lines.append(f"- `{field_name}`")
    lines.extend(
        [
            "",
            "## Required Run Summary Fields",
            "",
        ]
    )
    for field_name in REQUIRED_RUN_FIELDS:
        lines.append(f"- `{field_name}`")
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- `raw_command` and `raw_response` must be preserved for each high-level protocol action.",
            "- `mode_exit_attempted` and `mode_exit_confirmed` must remain distinct.",
            "- `rollback_attempted` and `rollback_confirmed` must remain distinct.",
            "- If final mode cannot be confirmed, the run must be marked `unsafe=true`.",
            "",
        ]
    )
    return "\n".join(lines)


def build_online_evidence_summary(
    *,
    generated_at: str,
    head: str,
    last_run_summary: Mapping[str, Any] | None = None,
) -> str:
    latest_status = str((last_run_summary or {}).get("status") or "ONLINE_EVIDENCE_REQUIRED")
    latest_mode = str((last_run_summary or {}).get("mode") or "dry_run")
    latest_failure = str((last_run_summary or {}).get("failure_reason") or "").strip()
    latest_run_is_real = latest_mode == "real_device"
    latest_mode_exit_confirmed = bool((last_run_summary or {}).get("mode_exit_confirmed", False))
    latest_rollback_confirmed = bool((last_run_summary or {}).get("rollback_confirmed", False))
    latest_unsafe = bool((last_run_summary or {}).get("unsafe", False))
    lines = [
        "# Online Evidence Summary",
        "",
        f"- generated_at: {generated_at}",
        f"- head: `{head}`",
        "- offline_fault_injection = PASS",
        "- real_device_abnormal_recovery = ONLINE_EVIDENCE_REQUIRED",
        f"- latest_status: {latest_status}",
        f"- latest_mode: {latest_mode}",
        "",
        "## Boundary",
        "",
        f"- {V1_CO2_ONLY_H2O_NOT_SUPPORTED_MESSAGE}",
        "- Dry-run artifacts do not count as real acceptance evidence.",
        "",
        "## Code / Offline Proven",
        "",
        "- CO2 zero/span main chain is covered in code and offline acceptance artifacts.",
        "- H2O zero/span is explicitly NOT_SUPPORTED on this HEAD; no H2O online acceptance should be attempted.",
        "- Shared writeback helper already proves offline: snapshot -> write -> GETCO readback -> mismatch rollback -> finally restore mode.",
        "- Offline fault injection already proves attempted-vs-confirmed mode exit semantics and unsafe marking.",
        "",
        "## Real-Device Proven",
        "",
    ]
    if latest_run_is_real and latest_status == "SUCCESS" and not latest_unsafe and latest_mode_exit_confirmed:
        lines.extend(
            [
                "- A real-device run has been captured with final mode exit confirmed.",
                "- Review the linked run summary and protocol log before promoting this beyond engineering evidence.",
                "",
            ]
        )
    else:
        lines.extend(
            [
                "- No real-device run has yet produced confirmed abnormal-recovery evidence that closes this item.",
                "",
            ]
        )
    lines.extend(
        [
            "## Missing / Pending Real-Device Evidence",
            "",
            "- A real-device `online_run_*.json` summary with complete required fields.",
            "- A matching `online_protocol_*.jsonl` log preserving `raw_command` and `raw_response` for the actual bench session.",
            "- A run where final mode exit is both attempted and confirmed on the real device.",
            "- If rollback is triggered on bench, a run where rollback attempt and rollback confirmation are both evidenced.",
            "",
            "## Latest Run",
            "",
        ]
    )
    if last_run_summary:
        lines.append(f"- run_id: `{last_run_summary.get('run_id', '')}`")
        lines.append(f"- session_id: `{last_run_summary.get('session_id', '')}`")
        lines.append(f"- device_id: `{last_run_summary.get('device_id', '')}`")
        lines.append(f"- unsafe: `{latest_unsafe}`")
        lines.append(f"- mode_exit_confirmed: `{latest_mode_exit_confirmed}`")
        lines.append(f"- rollback_confirmed: `{latest_rollback_confirmed}`")
        lines.append(f"- failure_reason: {latest_failure or '(none)'}")
        if not latest_run_is_real:
            lines.append("- assessment: This is still dry-run/template-only evidence, so the overall item remains `ONLINE_EVIDENCE_REQUIRED`.")
        elif latest_unsafe or not latest_mode_exit_confirmed:
            lines.append("- assessment: The run must be treated as `FAILED` for this session and the overall item remains `ONLINE_EVIDENCE_REQUIRED`.")
        elif latest_status != "SUCCESS":
            lines.append("- assessment: Real-device evidence was attempted, but the run did not close the item; overall status remains `ONLINE_EVIDENCE_REQUIRED`.")
        else:
            lines.append("- assessment: A real-device run was captured successfully, but manual engineering review is still required before changing the acceptance state.")
        run_path = str(last_run_summary.get("run_summary_path") or "").strip()
        protocol_path = str(last_run_summary.get("protocol_log_path") or "").strip()
        if run_path:
            lines.append(f"- run_summary_path: `{run_path}`")
        if protocol_path:
            lines.append(f"- protocol_log_path: `{protocol_path}`")
    else:
        lines.append("- No real-device run has been captured yet. Current directory only contains guarded templates and schema.")
    lines.append("")
    return "\n".join(lines)


def write_online_acceptance_bundle(
    output_dir: str | Path,
    *,
    generated_at: Optional[str] = None,
    head: Optional[str] = None,
    last_run_summary: Mapping[str, Any] | None = None,
) -> Dict[str, Path]:
    target_dir = Path(output_dir).resolve()
    target_dir.mkdir(parents=True, exist_ok=True)
    stamp = generated_at or _now_iso()
    commit = head or _head_commit()
    checklist_path = target_dir / "01_online_acceptance_checklist.md"
    template_path = target_dir / "02_online_run_template.json"
    schema_path = target_dir / "03_online_protocol_log_schema.md"
    summary_path = target_dir / "04_online_evidence_summary.md"
    _write_text(checklist_path, build_online_acceptance_checklist(generated_at=stamp, head=commit))
    _write_json(template_path, build_online_run_template(generated_at=stamp, head=commit))
    _write_text(schema_path, build_online_protocol_log_schema(generated_at=stamp, head=commit))
    _write_text(
        summary_path,
        build_online_evidence_summary(
            generated_at=stamp,
            head=commit,
            last_run_summary=last_run_summary,
        ),
    )
    return {
        "output_dir": target_dir,
        "checklist_path": checklist_path,
        "template_path": template_path,
        "schema_path": schema_path,
        "summary_path": summary_path,
    }


def _build_run_summary(
    *,
    run_id: str,
    session_id: str,
    device_id: str,
    start_ts: str,
    end_ts: str,
    result: Mapping[str, Any],
) -> Dict[str, Any]:
    detail_rows = [dict(row) for row in list(result.get("detail_rows") or [])]
    coeff_before = {str(row.get("group")): list(row.get("coeff_before") or []) for row in detail_rows}
    coeff_target = {str(row.get("group")): list(row.get("coeff_target") or []) for row in detail_rows}
    coeff_readback = {str(row.get("group")): list(row.get("coeff_readback") or []) for row in detail_rows}
    coeff_rollback_target = {
        str(row.get("group")): list(row.get("coeff_rollback_target") or [])
        for row in detail_rows
        if list(row.get("coeff_rollback_target") or [])
    }
    coeff_rollback_readback = {
        str(row.get("group")): list(row.get("coeff_rollback_readback") or [])
        for row in detail_rows
        if list(row.get("coeff_rollback_readback") or [])
    }
    return {
        "run_id": run_id,
        "session_id": session_id,
        "device_id": device_id,
        "start_ts": start_ts,
        "end_ts": end_ts,
        "mode_before": result.get("mode_before"),
        "mode_after": result.get("mode_after"),
        "mode_exit_attempted": bool(result.get("mode_exit_attempted", False)),
        "mode_exit_confirmed": bool(result.get("mode_exit_confirmed", False)),
        "coeff_before": coeff_before,
        "coeff_target": coeff_target,
        "coeff_readback": coeff_readback,
        "coeff_rollback_target": coeff_rollback_target,
        "coeff_rollback_readback": coeff_rollback_readback,
        "rollback_attempted": bool(result.get("rollback_attempted", False)),
        "rollback_confirmed": bool(result.get("rollback_confirmed", False)),
        "write_status": result.get("write_status"),
        "verify_status": result.get("verify_status"),
        "rollback_status": result.get("rollback_status"),
        "unsafe": bool(result.get("unsafe", False)),
        "failure_reason": str(result.get("failure_reason") or ""),
    }


def run_online_acceptance(
    *,
    config_path: str,
    analyzer: str = "",
    groups: Sequence[int] | None = None,
    gas_type: str = "co2",
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    real_device: bool = False,
    coeff_json: Optional[str] = None,
    allow_modified_write: bool = False,
    env: Mapping[str, str] | None = None,
    log_fn: Callable[[str], None] = _log,
    analyzer_factory: Optional[Callable[..., Any]] = None,
) -> Dict[str, Any]:
    effective_env = dict(os.environ if env is None else env)
    cfg = load_config(config_path)
    coeff_cfg = cfg.get("coefficients", {}) if isinstance(cfg.get("coefficients", {}), dict) else {}
    selected_groups = _normalize_groups(list(groups or [1]))
    capability = _ensure_co2_only_request(coeff_cfg, gas_type=gas_type, groups=selected_groups)
    generated_at = _now_iso()
    head = _head_commit()
    output_paths = write_online_acceptance_bundle(output_dir, generated_at=generated_at, head=head)

    env_gate = _safe_bool(effective_env.get(REAL_DEVICE_ENV))
    real_write_enabled = bool(real_device) and env_gate
    gate_state = {
        "cli_real_device": bool(real_device),
        REAL_DEVICE_ENV: env_gate,
        "real_device_write_enabled": real_write_enabled,
    }
    log_fn(
        "V1 online acceptance gates: "
        f"cli_real_device={gate_state['cli_real_device']} "
        f"{REAL_DEVICE_ENV}={gate_state[REAL_DEVICE_ENV]} "
        f"real_device_write_enabled={gate_state['real_device_write_enabled']}"
    )
    log_fn(
        "Capability boundary: "
        f"H2O zero/span status={capability['status']} note={capability['note']}"
    )

    if coeff_json and not allow_modified_write:
        raise RuntimeError(
            "run_v1_online_acceptance: refusing modified write payload without --allow-modified-write."
        )

    dry_run_summary: Dict[str, Any] = {
        "run_id": "",
        "session_id": "",
        "device_id": "",
        "mode": "dry_run",
        "status": "DRY_RUN_ONLY",
        "unsafe": False,
        "failure_reason": (
            ""
            if not bool(real_device)
            else f"Real-device gate incomplete; both --real-device and {REAL_DEVICE_ENV}=1 are required."
            if not real_write_enabled
            else ""
        ),
        "groups": selected_groups,
        "gas_type": "co2",
        "gates": gate_state,
        "capability": capability,
    }
    if not real_write_enabled:
        write_online_acceptance_bundle(
            output_dir,
            generated_at=generated_at,
            head=head,
            last_run_summary=dry_run_summary,
        )
        return {
            **dry_run_summary,
            **{key: str(value) for key, value in output_paths.items()},
        }

    analyzer_cfg = _resolve_analyzer_cfg(cfg, analyzer=analyzer)
    device_id = str(analyzer_cfg.get("device_id", "000") or "000")
    run_id = f"online_acceptance_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    session_id = run_id
    factory = analyzer_factory or GasAnalyzer
    analyzer_obj = factory(
        analyzer_cfg["port"],
        analyzer_cfg.get("baud", 115200),
        device_id=device_id,
    )
    recorder = ProtocolRecordingAnalyzer(analyzer_obj, session_id=session_id)
    protocol_log_path = Path(output_dir).resolve() / f"online_protocol_{run_id}.jsonl"
    run_summary_path = Path(output_dir).resolve() / f"online_run_{run_id}.json"
    start_ts = _now_iso()

    try:
        open_fn = getattr(analyzer_obj, "open", None)
        if callable(open_fn):
            open_fn()
        coeff_before = {
            int(group): [
                float(value)
                for _key, value in sorted(
                    recorder.read_coefficient_group(int(group)).items(),
                    key=lambda item: int(str(item[0]).lstrip("C")),
                )
            ]
            for group in selected_groups
        }
        target_groups = _load_target_groups(coeff_json) if coeff_json else coeff_before
        if sorted(target_groups.keys()) != sorted(selected_groups):
            raise RuntimeError(
                f"run_v1_online_acceptance: target groups {sorted(target_groups.keys())} "
                f"do not match selected groups {selected_groups}."
            )
        result = write_senco_groups_with_full_verification(
            recorder,
            expected_groups=target_groups,
        )
        end_ts = _now_iso()
        summary = _build_run_summary(
            run_id=run_id,
            session_id=session_id,
            device_id=device_id,
            start_ts=start_ts,
            end_ts=end_ts,
            result=result,
        )
        summary.update(
            {
                "mode": "real_device",
                "status": "SUCCESS" if bool(result.get("ok", False)) and not bool(result.get("unsafe", False)) else "FAILED",
                "gas_type": "co2",
                "groups": selected_groups,
                "gates": gate_state,
                "capability": capability,
                "run_summary_path": str(run_summary_path),
                "protocol_log_path": str(protocol_log_path),
            }
        )
        _write_json(run_summary_path, summary)
        _write_jsonl(protocol_log_path, recorder.protocol_rows)
        write_online_acceptance_bundle(
            output_dir,
            generated_at=end_ts,
            head=head,
            last_run_summary=summary,
        )
        return summary
    finally:
        close_fn = getattr(analyzer_obj, "close", None)
        if callable(close_fn):
            try:
                close_fn()
            except Exception:
                pass


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Guarded V1 online acceptance entry.")
    parser.add_argument("--config", default="configs/default_config.json")
    parser.add_argument("--analyzer", default="", help="Analyzer label, device ID, or 1-based index.")
    parser.add_argument("--groups", default="1", help="CO2 coefficient groups only, comma-separated (supported: 1,3).")
    parser.add_argument("--gas-type", default="co2", help="Requested capability gas type. Any non-CO2 request fails fast.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--real-device", action="store_true", help="First gate for real-device online acceptance.")
    parser.add_argument("--coeff-json", default=None, help="Optional target coefficient payload JSON for the selected CO2 groups.")
    parser.add_argument(
        "--allow-modified-write",
        action="store_true",
        help="Required together with --coeff-json to avoid accidental modified writes.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        result = run_online_acceptance(
            config_path=args.config,
            analyzer=args.analyzer,
            groups=_parse_group_list(args.groups),
            gas_type=args.gas_type,
            output_dir=args.output_dir,
            real_device=bool(args.real_device),
            coeff_json=args.coeff_json,
            allow_modified_write=bool(args.allow_modified_write),
        )
    except Exception as exc:
        _log(str(exc))
        return 2
    _log(
        "Online acceptance summary: "
        f"mode={result.get('mode')} status={result.get('status')} "
        f"unsafe={result.get('unsafe')} failure_reason={result.get('failure_reason', '')}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
