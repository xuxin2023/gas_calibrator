"""Offline repair plan for V1.5 CO2 SENCO1/SENCO3 mixed-layer states.

This module only reads prior evidence artifacts and writes review artifacts. It
does not open COM ports, control gas or water routes, or write coefficients.
"""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from ..senco_format import format_senco_values


@dataclass(frozen=True)
class Senco13RepairInputs:
    original_getco_snapshot_csv: Path
    first_pair_write_summary_csv: Path
    latest_s1_write_summary_csv: Path
    integrated_recalc_summary_csv: Path
    preclear_senco5_snapshot_csv: Optional[Path] = None
    postclear_senco5_snapshot_csv: Optional[Path] = None
    target_scenario: str = "force_neutral_senco5"
    target_senco5: Tuple[float, float] = (0.0, 1.0)
    target_device_ids: Tuple[str, ...] = ("022", "030", "033", "051")


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _device_id(value: Any) -> str:
    text = str(value or "").strip().upper()
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
        out.setdefault(device, {})[group] = values
    return out


def _load_first_pair_summary(path: str | Path) -> Dict[str, Dict[str, List[float]]]:
    out: Dict[str, Dict[str, List[float]]] = {}
    for row in _read_csv(path):
        device = _device_id(row.get("analyzer_device_id") or row.get("device_id"))
        if not device:
            continue
        s1 = _parse_values(row.get("senco1_readback")) or _parse_values(row.get("target_senco1_values")) or _parse_values(
            row.get("candidate_senco1_values")
        )
        s3 = _parse_values(row.get("senco3_readback")) or _parse_values(row.get("target_senco3_values")) or _parse_values(
            row.get("candidate_senco3_values")
        )
        out[device] = {
            "senco1": s1,
            "senco3": s3,
        }
    return out


def _load_latest_s1_summary(path: str | Path) -> Dict[str, Dict[str, List[float]]]:
    out: Dict[str, Dict[str, List[float]]] = {}
    for row in _read_csv(path):
        device = _device_id(row.get("analyzer_device_id") or row.get("device_id"))
        if not device:
            continue
        out[device] = {
            "senco1": _parse_values(row.get("senco1_readback")) or _parse_values(row.get("target_senco1_values")),
            "senco3": _parse_values(row.get("senco3_readback")) or _parse_values(row.get("old_senco3_values")),
        }
    return out


def _load_integrated_targets(path: str | Path, *, scenario: str) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in _read_csv(path):
        if str(row.get("scenario") or "") != scenario:
            continue
        device = _device_id(row.get("device_id") or row.get("analyzer_device_id"))
        if not device:
            continue
        primary = _parse_values(row.get("rounded_primary_payload_json"))
        secondary = _parse_values(row.get("rounded_secondary_payload_json"))
        if not primary or not secondary:
            continue
        out[device] = {
            "senco1": primary,
            "senco3": secondary,
            "rmse_ppm": row.get("rounded_rmse_ppm") or row.get("rmse_ppm"),
            "max_abs_error_ppm": row.get("rounded_max_abs_error_ppm") or row.get("max_abs_error_ppm"),
            "fit_point_count": row.get("fit_point_count"),
            "strategy": row.get("fit_strategy"),
            "scenario": scenario,
        }
    return out


def build_co2_senco13_repair_plan_tables(inputs: Senco13RepairInputs) -> Dict[str, List[Dict[str, Any]]]:
    target_devices = tuple(_device_id(device) for device in inputs.target_device_ids)
    original = _load_snapshot_by_group(inputs.original_getco_snapshot_csv)
    preclear_s5 = _load_snapshot_by_group(inputs.preclear_senco5_snapshot_csv)
    postclear_s5 = _load_snapshot_by_group(inputs.postclear_senco5_snapshot_csv)
    first_pair = _load_first_pair_summary(inputs.first_pair_write_summary_csv)
    latest_s1 = _load_latest_s1_summary(inputs.latest_s1_write_summary_csv)
    targets = _load_integrated_targets(inputs.integrated_recalc_summary_csv, scenario=inputs.target_scenario)
    target_s5 = [float(inputs.target_senco5[0]), float(inputs.target_senco5[1])]

    history_rows: List[Dict[str, Any]] = []
    repair_rows: List[Dict[str, Any]] = []
    command_rows: List[Dict[str, Any]] = []

    for device in target_devices:
        original_groups = original.get(device, {})
        target = targets.get(device, {})
        latest = latest_s1.get(device, {})
        first = first_pair.get(device, {})
        current_s5 = postclear_s5.get(device, {}).get("5", [])
        old_s5 = preclear_s5.get(device, {}).get("5", [])
        target_s1 = target.get("senco1", [])
        target_s3 = target.get("senco3", [])

        latest_s1_values = latest.get("senco1", [])
        latest_s3_values = latest.get("senco3", [])
        if target_s1 and target_s3:
            status = (
                "ready_for_live_precheck_then_pair_rewrite"
                if not current_s5 or _same_linear_layer(current_s5, target_s5)
                else "requires_senco5_layer_alignment_then_pair_rewrite"
            )
        else:
            status = "blocked_no_target_coefficients"
        mixed_state = bool(latest_s1_values and latest_s3_values)
        reason = (
            "Later evidence shows SENCO1 was rewritten while SENCO3 was preserved. "
            "The repair must overwrite SENCO1 and SENCO3 as one full-temperature pair after matching the reviewed SENCO5 final affine layer."
            if mixed_state
            else "No later S1-only write evidence found; still require live GETCO1/3/5 before any write."
        )

        history_rows.append(
            {
                "device_id": device,
                "original_senco1_before_first_pair_write": _values_json(original_groups.get("1", [])),
                "original_senco3_before_first_pair_write": _values_json(original_groups.get("3", [])),
                "preclear_senco5": _values_json(old_s5),
                "postclear_senco5_snapshot": _values_json(current_s5),
                "first_pair_senco1": _values_json(first.get("senco1", [])),
                "first_pair_senco3": _values_json(first.get("senco3", [])),
                "latest_s1_only_senco1": _values_json(latest_s1_values),
                "latest_s1_only_preserved_senco3": _values_json(latest_s3_values),
                "target_senco5": _values_json(target_s5),
                "senco5_layer_changed_to_target": bool(current_s5 and not _same_linear_layer(current_s5, target_s5)),
                "inferred_current_state": "mixed_s1_only_latest_plus_preserved_s3" if mixed_state else "unknown_live_read_required",
            }
        )
        repair_rows.append(
            {
                "device_id": device,
                "repair_status": status,
                "target_scenario": inputs.target_scenario,
                "target_senco5": _values_json(target_s5),
                "target_senco1": _values_json(target_s1),
                "target_senco3": _values_json(target_s3),
                "target_senco1_scientific_payload": ",".join(format_senco_values(target_s1)) if target_s1 else "",
                "target_senco3_scientific_payload": ",".join(format_senco_values(target_s3)) if target_s3 else "",
                "fit_point_count": target.get("fit_point_count", ""),
                "rounded_rmse_ppm": target.get("rmse_ppm", ""),
                "rounded_max_abs_error_ppm": target.get("max_abs_error_ppm", ""),
                "repair_reason": reason,
                "live_precheck_required": "GETCO1,GETCO3,GETCO5 current readback plus identity check before write",
                "physical_meaning": (
                    "SENCO1 carries ratio polynomial terms and SENCO3 carries temperature coupling terms. "
                    "They are one optical-temperature model and must be written as a matched pair; SENCO5 remains a separate final affine layer."
                ),
                "opens_com_ports": False,
                "controls_water_or_gas_routes": False,
                "writes_coefficients": False,
            }
        )
        if status != "blocked_no_target_coefficients":
            command_rows.extend(
                [
                    {
                        "device_id": device,
                        "sequence": 1,
                        "phase": "precheck",
                        "operation": "read_identity_and_getco",
                        "command_template": "GETCO,YGAS,FFF,1 / GETCO,YGAS,FFF,3 / GETCO,YGAS,FFF,5",
                        "allowed_now": False,
                        "reason": "offline plan only; live read must be explicitly triggered later",
                    },
                    {
                        "device_id": device,
                        "sequence": 2,
                        "phase": "pair_write",
                        "operation": "write_senco1",
                        "command_template": "SENCO1,YGAS,FFF," + ",".join(format_senco_values(target_s1)),
                        "allowed_now": False,
                        "reason": "write main CO2 ratio polynomial first after live precheck and operator approval",
                    },
                    {
                        "device_id": device,
                        "sequence": 3,
                        "phase": "pair_write",
                        "operation": "write_senco3",
                        "command_template": "SENCO3,YGAS,FFF," + ",".join(format_senco_values(target_s3)),
                        "allowed_now": False,
                        "reason": "write CO2 temperature coupling as the matched companion to SENCO1",
                    },
                    {
                        "device_id": device,
                        "sequence": 4,
                        "phase": "linear_layer",
                        "operation": "align_senco5_final_affine_layer",
                        "command_template": (
                            "if target GETCO5 == [0,1], use CLEARSENCO5,YGAS,FFF then read back GETCO5; "
                            "otherwise write SENCO5,YGAS,FFF,"
                            + ",".join(format_senco_values(target_s5))
                            + " then read back GETCO5"
                        ),
                        "allowed_now": False,
                        "reason": "SENCO5 is the final CO2 affine trim and is applied after the SENCO1/SENCO3 main chain",
                    },
                    {
                        "device_id": device,
                        "sequence": 5,
                        "phase": "readback",
                        "operation": "verify_senco1_senco3_senco5",
                        "command_template": "GETCO,YGAS,FFF,1 / GETCO,YGAS,FFF,3 / GETCO,YGAS,FFF,5",
                        "allowed_now": False,
                        "reason": "readback must match SENCO-rounded payloads before verification sampling",
                    },
                ]
            )

    database_sidecar_rows = [
        {
            "db_table": "coefficient_candidates",
            "record_key": f"co2_senco13_repair_{row['device_id']}",
            "component": "co2",
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
            "record_key": f"co2_senco13_repair_history_{row['device_id']}",
            "component": "co2",
            "analyzer_device_id": row["device_id"],
            "event_type": "offline_mixed_layer_repair_plan",
            "evidence_source": "offline_repair_review",
        }
        for row in history_rows
    )

    return {
        "co2_senco13_repair_history": history_rows,
        "co2_senco13_repair_plan": repair_rows,
        "co2_senco13_repair_command_plan": command_rows,
        "co2_senco13_repair_database_sidecar_rows": database_sidecar_rows,
    }


def write_co2_senco13_repair_plan_report(*, inputs: Senco13RepairInputs, output_dir: str | Path) -> Dict[str, Path]:
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    tables = build_co2_senco13_repair_plan_tables(inputs)
    outputs: Dict[str, Path] = {}
    for name, rows in tables.items():
        outputs[name] = _write_csv(output / f"{name}.csv", rows)
    sidecar = {
        "tool": "co2_senco13_repair_plan",
        "created_at": _now(),
        "no_write": True,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "database_target_tables": ["coefficient_candidates", "audit_events"],
        "suggested_rows": tables["co2_senco13_repair_database_sidecar_rows"],
    }
    sidecar_path = output / "co2_senco13_repair_database_sidecar.json"
    sidecar_path.write_text(json.dumps(sidecar, ensure_ascii=False, indent=2), encoding="utf-8")
    outputs["database_sidecar"] = sidecar_path
    meta = {
        "tool": "co2_senco13_repair_plan",
        "created_at": _now(),
        "target_scenario": inputs.target_scenario,
        "target_senco5": list(inputs.target_senco5),
        "target_device_ids": list(inputs.target_device_ids),
        "inputs": {
            "original_getco_snapshot_csv": str(inputs.original_getco_snapshot_csv.resolve()),
            "first_pair_write_summary_csv": str(inputs.first_pair_write_summary_csv.resolve()),
            "latest_s1_write_summary_csv": str(inputs.latest_s1_write_summary_csv.resolve()),
            "integrated_recalc_summary_csv": str(inputs.integrated_recalc_summary_csv.resolve()),
            "preclear_senco5_snapshot_csv": str(inputs.preclear_senco5_snapshot_csv.resolve()) if inputs.preclear_senco5_snapshot_csv else "",
            "postclear_senco5_snapshot_csv": str(inputs.postclear_senco5_snapshot_csv.resolve()) if inputs.postclear_senco5_snapshot_csv else "",
        },
        "boundary": {
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
        },
    }
    meta_path = output / "co2_senco13_repair_meta.json"
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    outputs["meta"] = meta_path
    outputs["markdown"] = _write_markdown(output / "co2_senco13_repair_plan.md", tables)
    return outputs


def _write_markdown(path: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> Path:
    lines = [
        "# V1.5 CO2 SENCO1/SENCO3 Repair Plan",
        "",
        "- Boundary: offline review only; no COM, no gas/water route control, no coefficient write.",
        "- Diagnosis: the latest evidence contains SENCO1-only writes while SENCO3 was preserved.",
        "- Repair contract: write SENCO1 and SENCO3 as one matched full-temperature pair first, then make SENCO5 match the reviewed final affine layer target.",
        "",
        "| Device | Status | RMSE ppm | Max ppm | Required action |",
        "| --- | --- | ---: | ---: | --- |",
    ]
    for row in tables.get("co2_senco13_repair_plan", []):
        status = str(row.get("repair_status", ""))
        if status == "blocked_no_target_coefficients":
            action = "blocked until target S1/S3 are regenerated"
        elif status == "requires_senco5_layer_alignment_then_pair_rewrite":
            action = "matched S1/S3 pair rewrite, then align reviewed SENCO5 final affine layer"
        else:
            action = "live GETCO1/3/5 precheck, matched S1/S3 pair rewrite, then reviewed S5 layer alignment"
        lines.append(
            "| {device} | {status} | {rmse} | {maxerr} | {action} |".format(
                device=row.get("device_id", ""),
                status=status,
                rmse=row.get("rounded_rmse_ppm", ""),
                maxerr=row.get("rounded_max_abs_error_ppm", ""),
                action=action,
            )
        )
    lines.extend(
        [
            "",
            "## Physical Meaning",
            "",
            "SENCO1 carries the CO2 ratio polynomial. SENCO3 carries temperature coupling terms for the same optical-temperature model. "
            "A later SENCO1-only correction can make a narrow current-state point look better but breaks the all-temperature pairing, so the safe repair is a full pair overwrite after a live readback gate.",
            "",
            "## Live Safety Gate",
            "",
            "Before any controlled write, read device identity and GETCO1/GETCO3/GETCO5 again. Write the matched SENCO1/SENCO3 main chain first with slow inter-command delays, then make GETCO5 match the reviewed target as a separate final affine layer and verify readback.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path
