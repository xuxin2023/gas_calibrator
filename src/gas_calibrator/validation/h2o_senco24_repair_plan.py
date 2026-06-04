"""Offline repair plan for V1.5 H2O SENCO2/SENCO4/SENCO6 layer states.

This module only reads prior evidence artifacts and writes review artifacts. It
does not open COM ports, control gas or water routes, or write coefficients.
"""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from ..senco_format import format_senco_values


@dataclass(frozen=True)
class H2OSenco24RepairInputs:
    original_getco_snapshot_csv: Path
    current_getco_snapshot_csv: Path
    candidate_device_policy_csv: Path
    candidate_payload_preview_csv: Path
    candidate_residuals_csv: Optional[Path] = None
    target_senco6: Tuple[float, float] = (0.0, 1.0)
    target_device_ids: Tuple[str, ...] = ("022", "030", "033", "051")


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _device_id(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.startswith("GA"):
        text = text[2:]
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _read_csv(path: str | Path | None) -> List[Dict[str, Any]]:
    if not path:
        return []
    source = Path(path)
    if not source.exists():
        return []
    with source.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers: List[str] = []
    for row in rows:
        for key in row:
            if key not in headers:
                headers.append(str(key))
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows([dict(row) for row in rows])
    return path


def _parse_values(value: Any) -> List[float]:
    if value in (None, "", "[]"):
        return []
    parsed: Any = None
    if isinstance(value, str):
        text = value.strip()
        try:
            parsed = json.loads(text)
        except Exception:
            parsed = [item.strip() for item in text.strip("[]").split(",") if item.strip()]
    else:
        parsed = value
    if not isinstance(parsed, Sequence) or isinstance(parsed, (str, bytes)):
        return []
    out: List[float] = []
    for item in parsed:
        try:
            out.append(float(item))
        except Exception:
            return []
    return out


def _values_json(values: Sequence[float]) -> str:
    return json.dumps([float(value) for value in values], separators=(",", ":"))


def _is_neutral(values: Sequence[float], *, atol: float = 0.05) -> bool:
    return len(values) >= 2 and abs(float(values[0])) <= atol and abs(float(values[1]) - 1.0) <= atol


def _same_linear_layer(left: Sequence[float], right: Sequence[float], *, atol: float = 0.05) -> bool:
    return (
        len(left) >= 2
        and len(right) >= 2
        and abs(float(left[0]) - float(right[0])) <= atol
        and abs(float(left[1]) - float(right[1])) <= atol
    )


def _load_snapshot_by_group(path: str | Path | None) -> Dict[str, Dict[str, List[float]]]:
    out: Dict[str, Dict[str, List[float]]] = {}
    for row in _read_csv(path):
        device = _device_id(row.get("analyzer_device_id") or row.get("device_id"))
        group = str(row.get("getco_group") or row.get("group") or "").strip()
        if not device or not group:
            continue
        values = _parse_values(row.get("coefficient_values_json") or row.get("values_json"))
        if not values:
            continue
        # Later rows in a snapshot should win; this handles repeated GETCO6 readbacks.
        out.setdefault(device, {})[group] = values
    return out


def _load_policy(path: str | Path) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in _read_csv(path):
        device = _device_id(row.get("analyzer_device_id") or row.get("device_id"))
        if device:
            out[device] = row
    return out


def _load_payloads(path: str | Path) -> Dict[str, Dict[str, List[float]]]:
    out: Dict[str, Dict[str, List[float]]] = {}
    for row in _read_csv(path):
        device = _device_id(row.get("analyzer_device_id") or row.get("device_id"))
        if not device:
            continue
        out[device] = {
            "senco2": _parse_values(row.get("senco2_payload_values_json") or row.get("target_senco2")),
            "senco4": _parse_values(row.get("senco4_payload_values_json") or row.get("target_senco4")),
        }
    return out


def _rejected_points_by_device(path: str | Path | None) -> Dict[str, List[str]]:
    rejected: Dict[str, List[str]] = {}
    for row in _read_csv(path):
        if str(row.get("residual_role") or "") != "rejected_input":
            continue
        device = _device_id(row.get("analyzer_device_id") or row.get("device_id"))
        point = str(row.get("point_run_id") or row.get("point_id") or "").strip()
        reason = str(row.get("reject_reasons") or "").strip()
        if device and point:
            rejected.setdefault(device, []).append(f"{point}:{reason}" if reason else point)
    return rejected


def _status_for_device(
    *,
    policy: Mapping[str, Any],
    target_senco2: Sequence[float],
    target_senco4: Sequence[float],
    current_s6: Sequence[float],
    target_s6: Sequence[float],
) -> str:
    candidate_status = str(policy.get("candidate_status") or "").strip()
    if not target_senco2 or not target_senco4:
        return "blocked_no_target_coefficients"
    if candidate_status == "blocked":
        return "blocked_candidate_policy"
    if not current_s6:
        return "ready_after_live_getco6_precheck"
    if not _same_linear_layer(current_s6, target_s6):
        return "requires_senco6_layer_alignment_then_pair_rewrite"
    if "review_required" in candidate_status:
        return "review_required_before_pair_rewrite"
    if "final_output_blocked" in candidate_status:
        return "ready_with_mandatory_post_write_h2o_verification"
    return "ready_for_live_precheck_then_pair_rewrite"


def build_h2o_senco24_repair_plan_tables(inputs: H2OSenco24RepairInputs) -> Dict[str, List[Dict[str, Any]]]:
    target_devices = tuple(_device_id(device) for device in inputs.target_device_ids)
    original = _load_snapshot_by_group(inputs.original_getco_snapshot_csv)
    current = _load_snapshot_by_group(inputs.current_getco_snapshot_csv)
    policies = _load_policy(inputs.candidate_device_policy_csv)
    payloads = _load_payloads(inputs.candidate_payload_preview_csv)
    rejected_points = _rejected_points_by_device(inputs.candidate_residuals_csv)
    target_s6 = [float(inputs.target_senco6[0]), float(inputs.target_senco6[1])]

    history_rows: List[Dict[str, Any]] = []
    repair_rows: List[Dict[str, Any]] = []
    command_rows: List[Dict[str, Any]] = []

    for device in target_devices:
        original_groups = original.get(device, {})
        current_groups = current.get(device, {})
        policy = policies.get(device, {})
        payload = payloads.get(device, {})
        target_senco2 = payload.get("senco2", [])
        target_senco4 = payload.get("senco4", [])
        current_s6 = current_groups.get("6", [])
        original_s6 = original_groups.get("6", [])
        status = _status_for_device(
            policy=policy,
            target_senco2=target_senco2,
            target_senco4=target_senco4,
            current_s6=current_s6,
            target_s6=target_s6,
        )
        s6_layer_changed = bool(original_s6 and not _same_linear_layer(original_s6, target_s6))
        target_s6_neutral = _is_neutral(target_s6)

        history_rows.append(
            {
                "device_id": device,
                "original_senco2_before_h2o_write": _values_json(original_groups.get("2", [])),
                "original_senco4_before_h2o_write": _values_json(original_groups.get("4", [])),
                "original_senco6_before_h2o_write": _values_json(original_s6),
                "current_senco2_snapshot": _values_json(current_groups.get("2", [])),
                "current_senco4_snapshot": _values_json(current_groups.get("4", [])),
                "current_senco6_snapshot": _values_json(current_s6),
                "senco6_layer_changed_to_target": s6_layer_changed,
                "inferred_layer_state": (
                    "old_final_affine_layer_removed_requires_matched_main_chain"
                    if s6_layer_changed
                    else "neutral_final_affine_layer_contract"
                    if target_s6_neutral
                    else "custom_final_affine_layer_contract"
                ),
            }
        )
        repair_rows.append(
            {
                "device_id": device,
                "repair_status": status,
                "target_senco6": _values_json(target_s6),
                "target_senco2": _values_json(target_senco2),
                "target_senco4": _values_json(target_senco4),
                "target_senco2_scientific_payload": ",".join(format_senco_values(target_senco2)) if target_senco2 else "",
                "target_senco4_scientific_payload": ",".join(format_senco_values(target_senco4)) if target_senco4 else "",
                "candidate_status": policy.get("candidate_status", ""),
                "fit_point_count": policy.get("complete_point_count", ""),
                "rejected_point_count": policy.get("rejected_point_count", ""),
                "fit_rmse_mmol": policy.get("fit_rmse_mmol", ""),
                "fit_max_error_mmol": policy.get("fit_max_error_mmol", ""),
                "fit_max_abs_relative_error_pct": policy.get("fit_max_abs_relative_error_pct", ""),
                "reported_h2o_max_error_mmol_before_write": policy.get("reported_h2o_max_error_mmol_before_write", ""),
                "warning_reasons": policy.get("warning_reasons", ""),
                "blocked_reasons": policy.get("blocked_reasons", ""),
                "rejected_points": ";".join(rejected_points.get(device, [])),
                "repair_reason": (
                    "H2O fit uses factory-mode R_H2O and chamber temperature, so existing final output coefficients do not change the raw ratio. "
                    "SENCO6 is only the final affine H2O display layer; if it was cleared or changed, SENCO2/SENCO4 must be paired with that layer contract."
                ),
                "live_precheck_required": "GETCO2,GETCO4,GETCO6 current readback plus identity check before write",
                "physical_meaning": (
                    "SENCO2 carries H2O ratio polynomial terms and SENCO4 carries temperature coupling terms. "
                    "SENCO6 is a final output affine trim and must not be allowed to silently double-correct or un-correct the main model."
                ),
                "opens_com_ports": False,
                "controls_water_or_gas_routes": False,
                "writes_coefficients": False,
            }
        )
        if status != "blocked_candidate_policy" and target_senco2 and target_senco4:
            command_rows.extend(
                [
                    {
                        "device_id": device,
                        "sequence": 1,
                        "phase": "precheck",
                        "operation": "read_identity_and_getco",
                        "command_template": "GETCO,YGAS,FFF,2 / GETCO,YGAS,FFF,4 / GETCO,YGAS,FFF,6",
                        "allowed_now": False,
                        "reason": "offline plan only; live read must be explicitly triggered later",
                    },
                    {
                        "device_id": device,
                        "sequence": 2,
                        "phase": "pair_write",
                        "operation": "write_senco2",
                        "command_template": "SENCO2,YGAS,FFF," + ",".join(format_senco_values(target_senco2)),
                        "allowed_now": False,
                        "reason": "write main H2O ratio polynomial first after live precheck and operator approval",
                    },
                    {
                        "device_id": device,
                        "sequence": 3,
                        "phase": "pair_write",
                        "operation": "write_senco4",
                        "command_template": "SENCO4,YGAS,FFF," + ",".join(format_senco_values(target_senco4)),
                        "allowed_now": False,
                        "reason": "write H2O temperature coupling as the matched companion to SENCO2",
                    },
                    {
                        "device_id": device,
                        "sequence": 4,
                        "phase": "linear_layer",
                        "operation": "align_senco6_final_affine_layer",
                        "command_template": (
                            "if target GETCO6 == [0,1], use CLEARSENCO6,YGAS,FFF then read back GETCO6; "
                            "otherwise write SENCO6,YGAS,FFF,"
                            + ",".join(format_senco_values(target_s6))
                            + " then read back GETCO6"
                        ),
                        "allowed_now": False,
                        "reason": "SENCO6 is the final H2O affine trim and is applied after the SENCO2/SENCO4 main chain",
                    },
                    {
                        "device_id": device,
                        "sequence": 5,
                        "phase": "readback",
                        "operation": "verify_senco2_senco4_senco6",
                        "command_template": "GETCO,YGAS,FFF,2 / GETCO,YGAS,FFF,4 / GETCO,YGAS,FFF,6",
                        "allowed_now": False,
                        "reason": "readback must match SENCO-rounded payloads before verification sampling",
                    },
                ]
            )

    database_sidecar_rows = [
        {
            "db_table": "coefficient_candidates",
            "record_key": f"h2o_senco24_repair_{row['device_id']}",
            "component": "h2o",
            "analyzer_device_id": row["device_id"],
            "candidate_status": row["repair_status"],
            "auto_write_allowed": False,
            "evidence_source": "offline_repair_review",
        }
        for row in repair_rows
    ]
    database_sidecar_rows.extend(
        {
            "db_table": "audit_events",
            "record_key": f"h2o_senco24_repair_history_{row['device_id']}",
            "component": "h2o",
            "analyzer_device_id": row["device_id"],
            "event_type": "offline_senco6_layer_repair_plan",
            "evidence_source": "offline_repair_review",
        }
        for row in history_rows
    )

    return {
        "h2o_senco24_repair_history": history_rows,
        "h2o_senco24_repair_plan": repair_rows,
        "h2o_senco24_repair_command_plan": command_rows,
        "h2o_senco24_repair_database_sidecar_rows": database_sidecar_rows,
    }


def write_h2o_senco24_repair_plan_report(*, inputs: H2OSenco24RepairInputs, output_dir: str | Path) -> Dict[str, Path]:
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    tables = build_h2o_senco24_repair_plan_tables(inputs)
    outputs: Dict[str, Path] = {}
    for name, rows in tables.items():
        outputs[name] = _write_csv(output / f"{name}.csv", rows)
    sidecar = {
        "tool": "h2o_senco24_repair_plan",
        "created_at": _now(),
        "no_write": True,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "database_target_tables": ["coefficient_candidates", "audit_events"],
        "suggested_rows": tables["h2o_senco24_repair_database_sidecar_rows"],
    }
    sidecar_path = output / "h2o_senco24_repair_database_sidecar.json"
    sidecar_path.write_text(json.dumps(sidecar, ensure_ascii=False, indent=2), encoding="utf-8")
    outputs["database_sidecar"] = sidecar_path
    meta = {
        "tool": "h2o_senco24_repair_plan",
        "created_at": _now(),
        "target_senco6": list(inputs.target_senco6),
        "target_device_ids": list(inputs.target_device_ids),
        "inputs": {
            "original_getco_snapshot_csv": str(inputs.original_getco_snapshot_csv.resolve()),
            "current_getco_snapshot_csv": str(inputs.current_getco_snapshot_csv.resolve()),
            "candidate_device_policy_csv": str(inputs.candidate_device_policy_csv.resolve()),
            "candidate_payload_preview_csv": str(inputs.candidate_payload_preview_csv.resolve()),
            "candidate_residuals_csv": str(inputs.candidate_residuals_csv.resolve()) if inputs.candidate_residuals_csv else "",
        },
        "boundary": {
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
        },
    }
    meta_path = output / "h2o_senco24_repair_meta.json"
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    outputs["meta"] = meta_path
    outputs["markdown"] = _write_markdown(output / "h2o_senco24_repair_plan.md", tables)
    return outputs


def _write_markdown(path: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> Path:
    lines = [
        "# V1.5 H2O SENCO2/SENCO4 Repair Plan",
        "",
        "- Boundary: offline review only; no COM, no gas/water route control, no coefficient write.",
        "- Diagnosis: H2O candidates must be matched to the active SENCO6 final affine layer.",
        "- Repair contract: write SENCO2 and SENCO4 as one matched full-temperature pair first, then make SENCO6 match the reviewed final affine layer target.",
        "",
        "| Device | Status | Fit max rel % | Rejected points | Required action |",
        "| --- | --- | ---: | ---: | --- |",
    ]
    for row in tables.get("h2o_senco24_repair_plan", []):
        status = str(row.get("repair_status", ""))
        if status == "blocked_candidate_policy":
            action = "blocked until firmware/manual blocker is resolved; coefficients kept as future evidence"
        elif status == "ready_with_mandatory_post_write_h2o_verification":
            action = "live GETCO2/4/6 precheck, matched S2/S4 pair rewrite, reviewed S6 layer alignment, mandatory H2O verification"
        elif status == "requires_senco6_layer_alignment_then_pair_rewrite":
            action = "matched S2/S4 pair rewrite, then align reviewed SENCO6 final affine layer"
        else:
            action = "live GETCO2/4/6 precheck, matched S2/S4 pair rewrite, then reviewed S6 layer alignment"
        lines.append(
            "| {device} | {status} | {maxrel} | {rejects} | {action} |".format(
                device=row.get("device_id", ""),
                status=status,
                maxrel=row.get("fit_max_abs_relative_error_pct", ""),
                rejects=row.get("rejected_point_count", ""),
                action=action,
            )
        )
    lines.extend(
        [
            "",
            "## Physical Meaning",
            "",
            "The H2O fit uses factory-mode `R_H2O` and analyzer chamber temperature. Existing displayed H2O coefficients do not change the stored optical ratio, but SENCO6 can change the final H2O number shown by firmware. If SENCO6 is changed from a non-neutral layer to `[0,1]`, SENCO2/SENCO4 must be generated for the same neutral-layer contract.",
            "",
            "## Live Safety Gate",
            "",
            "Before any controlled write, read device identity and GETCO2/GETCO4/GETCO6 again. If GETCO6 is not `[0,1]`, neutralize it first under the controlled SENCO6 flow, then write the matched SENCO2/SENCO4 pair with slow inter-command delays and readback verification. A short open-flow H2O verification remains mandatory after write.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path
