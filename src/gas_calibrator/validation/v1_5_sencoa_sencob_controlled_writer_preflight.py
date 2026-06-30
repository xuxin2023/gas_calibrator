"""Offline V1.5 SENCOA/SENCOB controlled-writer preflight.

This module defines the future real-write boundary for R0(T) coefficient
groups. It never opens COM ports, sends analyzer commands, or writes
coefficients.
"""

from __future__ import annotations

import csv
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

from .v1_5_algorithm_route_profiles import load_v1_5_algorithm_route_profiles


MIN_COMMAND_GAP_S = 1.0
REQUIRED_GROUPS = ("SENCOA", "SENCOB")
READBACK_BY_GROUP = {"SENCOA": "GETCOA", "SENCOB": "GETCOB"}
PHYSICAL_BY_GROUP = {"SENCOA": "R0_CO2(T)", "SENCOB": "R0_H2O(T)"}
FUTURE_CONFIRMATION_TEXT = "WRITE_SENCOA_SENCOB_V1_5_R0_PAIR"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


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
        writer.writerows([dict(row) for row in rows])


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8-sig")


def _read_csv(path: str | Path) -> List[Dict[str, Any]]:
    with Path(path).open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _load_json(path: str | Path) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def _profile_by_id(config: Mapping[str, Any], profile_id: str) -> Mapping[str, Any]:
    for profile in config.get("profiles", []):
        if str(profile.get("profile_id") or "") == profile_id:
            return profile
    raise ValueError(f"Profile not found: {profile_id}")


def _safe_device_id(value: Any) -> str:
    text = str(value or "").strip()
    if text.isdigit():
        return f"{int(text):03d}"
    return text.upper()


def _finite_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    return numeric if math.isfinite(numeric) else None


def _payload_values(row: Mapping[str, Any], width: int) -> List[float]:
    raw = row.get("payload_values")
    if raw not in (None, ""):
        try:
            parsed = json.loads(str(raw))
            if isinstance(parsed, list):
                values = [_finite_float(item) for item in parsed]
                return [float(item) for item in values if item is not None]
        except Exception:
            pass
        values = [_finite_float(part.strip()) for part in str(raw).replace(";", ",").split(",")]
        return [float(item) for item in values if item is not None]
    values: List[float] = []
    for idx in range(int(width)):
        numeric = _finite_float(row.get(f"c{idx}"))
        if numeric is not None:
            values.append(float(numeric))
    return values


def _snapshot_records(raw: Any) -> List[Dict[str, Any]]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [dict(item) for item in raw if isinstance(item, Mapping)]
    if isinstance(raw, Mapping):
        rows: List[Dict[str, Any]] = []
        for key, value in raw.items():
            if isinstance(value, Mapping):
                item = dict(value)
                item.setdefault("analyzer_device_id", key)
                rows.append(item)
        return rows
    return []


def _snapshot_values(row: Mapping[str, Any], readback_group: str) -> List[float]:
    candidates = (
        f"{readback_group}_before",
        f"{readback_group}_old",
        readback_group,
        readback_group.lower(),
    )
    for key in candidates:
        if key in row:
            value = row.get(key)
            if isinstance(value, list):
                return [float(item) for item in value if _finite_float(item) is not None]
            if isinstance(value, str):
                try:
                    parsed = json.loads(value)
                    if isinstance(parsed, list):
                        return [float(item) for item in parsed if _finite_float(item) is not None]
                except Exception:
                    return [
                        float(item)
                        for item in (_finite_float(part.strip()) for part in value.replace(";", ",").split(","))
                        if item is not None
                    ]
    return []


def _gate(status: str, *, required: bool, evidence: str, gate: str) -> Dict[str, Any]:
    return {"gate": gate, "status": status, "required": required, "evidence": evidence}


def _payload_review_rows(
    payload_review_path: str | Path | None,
    *,
    payload_width: int,
) -> List[Dict[str, Any]]:
    if not payload_review_path:
        return [
            {
                "row_type": "template",
                "analyzer_device_id": "<device_id>",
                "sn_code": "<sn_code>",
                "coefficient_group": group,
                "readback_group": READBACK_BY_GROUP[group],
                "physical_quantity": PHYSICAL_BY_GROUP[group],
                "target": "<device_id>",
                "payload_values": json.dumps([f"c{idx}" for idx in range(payload_width)]),
                "payload_width": payload_width,
                "status": "template_only",
                "blocked_reasons": "payload_review_csv_not_supplied",
            }
            for group in REQUIRED_GROUPS
        ]

    rows = _read_csv(payload_review_path)
    out: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows, start=1):
        group = str(row.get("coefficient_group") or row.get("group") or "").strip().upper()
        target = str(row.get("target") or row.get("analyzer_device_id") or row.get("device_id") or "").strip()
        values = _payload_values(row, payload_width)
        reasons: List[str] = []
        if group not in REQUIRED_GROUPS:
            reasons.append("unsupported_group")
        if len(values) != payload_width:
            reasons.append(f"payload_width_{len(values)}_expected_{payload_width}")
        if target.upper() == "FFF":
            reasons.append("broadcast_target_not_allowed_for_default_controlled_write")
        if not target:
            reasons.append("missing_target")
        if not _safe_device_id(row.get("analyzer_device_id") or row.get("device_id") or target):
            reasons.append("missing_analyzer_device_id")
        out.append(
            {
                "row_index": idx,
                "analyzer_device_id": _safe_device_id(row.get("analyzer_device_id") or row.get("device_id") or target),
                "sn_code": str(row.get("sn_code") or row.get("device_code") or "").strip(),
                "coefficient_group": group,
                "readback_group": READBACK_BY_GROUP.get(group, ""),
                "physical_quantity": PHYSICAL_BY_GROUP.get(group, ""),
                "target": target,
                "payload_values": json.dumps(values, ensure_ascii=False),
                "payload_width": len(values),
                "source_model_hash": row.get("source_model_hash", ""),
                "status": "pass" if not reasons else "blocked",
                "blocked_reasons": ";".join(reasons),
            }
        )
    return out


def _snapshot_review_rows(
    old_snapshot_json: str | Path | None,
    *,
    payload_width: int,
) -> List[Dict[str, Any]]:
    if not old_snapshot_json:
        return [
            {
                "row_type": "template",
                "analyzer_device_id": "<device_id>",
                "sn_code": "<sn_code>",
                "GETCOA_before": "[a0,a1,a2,a3]",
                "GETCOB_before": "[b0,b1,b2,b3]",
                "status": "template_only",
                "blocked_reasons": "old_snapshot_json_not_supplied",
            }
        ]

    records = _snapshot_records(_load_json(old_snapshot_json))
    out: List[Dict[str, Any]] = []
    for idx, row in enumerate(records, start=1):
        device_id = _safe_device_id(row.get("analyzer_device_id") or row.get("device_id"))
        getcoa = _snapshot_values(row, "GETCOA")
        getcob = _snapshot_values(row, "GETCOB")
        reasons: List[str] = []
        if not device_id:
            reasons.append("missing_analyzer_device_id")
        if len(getcoa) < payload_width:
            reasons.append(f"GETCOA_snapshot_too_short_{len(getcoa)}")
        if len(getcob) < payload_width:
            reasons.append(f"GETCOB_snapshot_too_short_{len(getcob)}")
        out.append(
            {
                "row_index": idx,
                "analyzer_device_id": device_id,
                "sn_code": str(row.get("sn_code") or row.get("device_code") or "").strip(),
                "GETCOA_before": json.dumps(getcoa, ensure_ascii=False),
                "GETCOB_before": json.dumps(getcob, ensure_ascii=False),
                "raw_lines_present": bool(row.get("raw_lines") or row.get("raw")),
                "timestamps_present": bool(row.get("timestamps") or row.get("timestamp")),
                "payload_width_required": payload_width,
                "status": "pass" if not reasons else "blocked",
                "blocked_reasons": ";".join(reasons),
            }
        )
    return out


def _payload_group_set(rows: Sequence[Mapping[str, Any]]) -> set[str]:
    return {str(row.get("coefficient_group") or "").strip().upper() for row in rows if row.get("status") == "pass"}


def build_v1_5_sencoa_sencob_controlled_writer_preflight(
    profile_path: str | Path,
    *,
    payload_review_path: str | Path | None = None,
    old_snapshot_json: str | Path | None = None,
    profile_id: str = "absorption_ratio_shadow",
    future_command_gap_s: float = MIN_COMMAND_GAP_S,
) -> Dict[str, Any]:
    """Build no-write preflight tables for a future SENCOA/SENCOB writer."""

    if float(future_command_gap_s) < MIN_COMMAND_GAP_S:
        raise ValueError(
            f"SENCOA/SENCOB preflight refuses future command gap below {MIN_COMMAND_GAP_S:g}s"
        )
    config = load_v1_5_algorithm_route_profiles(profile_path)
    profile = _profile_by_id(config, profile_id)
    contract = profile.get("r0_write_contract", {})
    payload_width = int(contract.get("payload_width") or 4)
    min_gap_s = max(float(contract.get("minimum_serial_command_gap_s") or MIN_COMMAND_GAP_S), future_command_gap_s)

    payload_rows = _payload_review_rows(payload_review_path, payload_width=payload_width)
    snapshot_rows = _snapshot_review_rows(old_snapshot_json, payload_width=payload_width)
    payload_pass_groups = _payload_group_set(payload_rows)
    payload_status = "pass" if payload_pass_groups == set(REQUIRED_GROUPS) else "planned"
    if payload_review_path and payload_status != "pass":
        payload_status = "blocked"
    snapshot_status = "pass" if old_snapshot_json and snapshot_rows and all(
        row.get("status") == "pass" for row in snapshot_rows
    ) else ("planned" if not old_snapshot_json else "blocked")

    gates = [
        _gate(
            "pass",
            required=True,
            gate="offline_no_write_only",
            evidence="no COM imports, no GasAnalyzer use, writes_coefficients=false",
        ),
        _gate(
            "blocked",
            required=True,
            gate="real_writer_implementation",
            evidence=contract.get("writer_implementation_status", "missing_real_writer_design_only"),
        ),
        _gate(
            "pass",
            required=True,
            gate="serial_command_gap",
            evidence=f"future_command_gap_s={min_gap_s:g}; minimum={MIN_COMMAND_GAP_S:g}",
        ),
        _gate(
            payload_status,
            required=True,
            gate="reviewed_payload_available",
            evidence=str(payload_review_path or "payload review CSV template emitted only"),
        ),
        _gate(
            snapshot_status,
            required=True,
            gate="old_getcoa_getcob_snapshot_available",
            evidence=str(old_snapshot_json or "old snapshot JSON template emitted only"),
        ),
        _gate(
            "planned",
            required=True,
            gate="readback_verification",
            evidence="future writer must read GETCOA/GETCOB after each write and compare against reviewed payload",
        ),
        _gate(
            "planned",
            required=True,
            gate="rollback_contract",
            evidence="future writer must rollback changed groups in reverse order using old GETCOA/GETCOB snapshot",
        ),
        _gate(
            "pass",
            required=True,
            gate="route_and_sampling_untouched",
            evidence="preflight does not modify formal CO2/H2O queues or shared sampling",
        ),
        _gate(
            "planned",
            required=True,
            gate="independent_no_write_reverification",
            evidence="future write acceptance requires separate CO2/H2O no-write reverification",
        ),
    ]

    write_steps = [
        {
            "step_order": 1,
            "phase": "review_inputs",
            "action": "load reviewed SENCOA/SENCOB payload rows",
            "preflight_executes_step": False,
            "future_writer_step_required": True,
            "exit_on_failure": True,
        },
        {
            "step_order": 2,
            "phase": "identity_binding",
            "action": "bind port, label, analyzer_device_id, sn_code, and algorithm profile",
            "preflight_executes_step": False,
            "future_writer_step_required": True,
            "exit_on_failure": True,
        },
        {
            "step_order": 3,
            "phase": "old_snapshot",
            "action": "read and archive GETCOA/GETCOB before any write",
            "preflight_executes_step": False,
            "future_writer_step_required": True,
            "exit_on_failure": True,
        },
        {
            "step_order": 4,
            "phase": "mode_and_pacing",
            "action": "enter MODE2 and enforce command gap >=1s",
            "preflight_executes_step": False,
            "future_writer_step_required": True,
            "exit_on_failure": True,
        },
        {
            "step_order": 5,
            "phase": "write_sencoa",
            "action": "write SENCOA payload, then read GETCOA until reviewed payload matches",
            "preflight_executes_step": False,
            "future_writer_step_required": True,
            "exit_on_failure": True,
        },
        {
            "step_order": 6,
            "phase": "write_sencob",
            "action": "write SENCOB payload, then read GETCOB until reviewed payload matches",
            "preflight_executes_step": False,
            "future_writer_step_required": True,
            "exit_on_failure": True,
        },
        {
            "step_order": 7,
            "phase": "rollback",
            "action": "on failure, restore changed groups from old snapshot in reverse_changed_order",
            "preflight_executes_step": False,
            "future_writer_step_required": True,
            "exit_on_failure": True,
        },
        {
            "step_order": 8,
            "phase": "post_write_evidence",
            "action": "archive readback, old snapshot, operator confirmation, reviewer and approver",
            "preflight_executes_step": False,
            "future_writer_step_required": True,
            "exit_on_failure": True,
        },
        {
            "step_order": 9,
            "phase": "independent_reverification",
            "action": "run separate CO2/H2O no-write reverification before production acceptance",
            "preflight_executes_step": False,
            "future_writer_step_required": True,
            "exit_on_failure": True,
        },
    ]

    manifest = {
        "schema_version": 1,
        "generated_at": _now(),
        "profile_id": profile_id,
        "preflight_scope": "sencoa_sencob_controlled_writer_no_write_preflight",
        "no_write": True,
        "opens_com_ports": False,
        "uses_gas_analyzer": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "not_real_acceptance_evidence": True,
        "real_write_unlock_status": "blocked_pending_real_writer_implementation",
        "future_confirmation_text": FUTURE_CONFIRMATION_TEXT,
        "payload_width": payload_width,
        "minimum_serial_command_gap_s": min_gap_s,
        "payload_review_path": str(payload_review_path or ""),
        "old_snapshot_json": str(old_snapshot_json or ""),
        "controlled_writer_preflight_tool": contract.get("controlled_writer_preflight_tool"),
    }
    return {
        "manifest": manifest,
        "preflight_gates": gates,
        "payload_review": payload_rows,
        "old_snapshot_review": snapshot_rows,
        "future_write_boundary": write_steps,
    }


def write_v1_5_sencoa_sencob_controlled_writer_preflight(
    profile_path: str | Path,
    output_dir: str | Path,
    *,
    payload_review_path: str | Path | None = None,
    old_snapshot_json: str | Path | None = None,
    profile_id: str = "absorption_ratio_shadow",
    future_command_gap_s: float = MIN_COMMAND_GAP_S,
) -> Dict[str, str]:
    tables = build_v1_5_sencoa_sencob_controlled_writer_preflight(
        profile_path,
        payload_review_path=payload_review_path,
        old_snapshot_json=old_snapshot_json,
        profile_id=profile_id,
        future_command_gap_s=future_command_gap_s,
    )
    out = Path(output_dir)
    outputs = {
        "manifest": out / "v1_5_sencoa_sencob_controlled_writer_preflight_manifest.json",
        "preflight_gates": out / "v1_5_sencoa_sencob_controlled_writer_preflight_gates.csv",
        "payload_review": out / "v1_5_sencoa_sencob_controlled_writer_payload_review.csv",
        "old_snapshot_review": out / "v1_5_sencoa_sencob_controlled_writer_old_snapshot_review.csv",
        "future_write_boundary": out / "v1_5_sencoa_sencob_controlled_writer_future_write_boundary.csv",
        "summary": out / "V1_5_SENCOA_SENCOB_CONTROLLED_WRITER_PREFLIGHT.md",
    }
    _write_json(outputs["manifest"], tables["manifest"])
    _write_csv(outputs["preflight_gates"], tables["preflight_gates"])
    _write_csv(outputs["payload_review"], tables["payload_review"])
    _write_csv(outputs["old_snapshot_review"], tables["old_snapshot_review"])
    _write_csv(outputs["future_write_boundary"], tables["future_write_boundary"])
    summary = [
        "# V1.5 SENCOA/SENCOB controlled-writer preflight",
        "",
        "This is a no-write preflight and future real-write boundary document.",
        "",
        "- It does not open COM ports, import GasAnalyzer, or write coefficients.",
        "- Real writing remains blocked until a controlled writer is implemented and reviewed.",
        "- Future payloads must contain SENCOA and SENCOB rows with four finite coefficients each.",
        "- Future write attempts must snapshot GETCOA/GETCOB first, verify readback after each write, and rollback on mismatch.",
        "- Analyzer command pacing must stay at or above 1 second.",
        "- Production acceptance still requires independent CO2/H2O no-write reverification after any future write.",
    ]
    outputs["summary"].write_text("\n".join(summary) + "\n", encoding="utf-8")
    return {key: str(path) for key, path in outputs.items()}
