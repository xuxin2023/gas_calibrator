"""Export a no-write V1.5 main SENCO write precheck package.

The package bridges residual-gated model-selection artifacts into controlled
SENCO1/3, SENCO2/4, and S5/S6 review surfaces. It never opens COM ports,
controls routes, or writes coefficients. The output is evidence for a later
controlled writer.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from ..validation.v1_5_artifact_hash_binding import write_artifact_hash_manifest

from ..senco_format import format_senco_values


PRIMARY_TERMS = ("intercept", "R", "R2", "R3")
SECONDARY_TERMS = ("T", "T2", "RT")
PRESSURE_TERMS = ("P", "RP", "RTP")
DEFAULT_INCLUDE_DEVICES = ("091", "077", "001", "084")
DEFAULT_RELATIVE_LIMIT_PCT = 10.0
FIT_INPUT_TRACEABILITY_BLOCKER = "fit_input_traceability_missing_or_invalid"
FIT_INPUT_SOURCE_FIELDS = (
    "fit_input_quality_summary_source",
    "fit_input_quality_devices_source",
)
FIT_INPUT_READY_STATUSES = {"usable_for_candidate_fit", "ready", "pass"}


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    keys: List[str] = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in keys})


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "None", "null"):
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def _component_channels(component: str) -> Tuple[int, int, int]:
    if component == "h2o":
        return 2, 4, 6
    return 1, 3, 5


def _load_json(path: Optional[Path]) -> Mapping[str, Any]:
    if not path:
        return {}
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _load_device_map(plan_path: Optional[Path]) -> Dict[str, Dict[str, Any]]:
    plan = _load_json(plan_path)
    devices = plan.get("devices")
    analyzers: Sequence[Any] = ()
    if isinstance(devices, Mapping):
        maybe = devices.get("gas_analyzers")
        if isinstance(maybe, Sequence) and not isinstance(maybe, (str, bytes)):
            analyzers = maybe
    out: Dict[str, Dict[str, Any]] = {}
    for item in analyzers:
        if not isinstance(item, Mapping):
            continue
        device_id = str(item.get("runtime_device_id") or item.get("device_id") or "").strip()
        if not device_id:
            continue
        out[device_id] = {
            "analyzer_prefix": item.get("name", ""),
            "port": item.get("port", ""),
            "configured_device_id": item.get("configured_device_id", ""),
            "runtime_identity_bound": item.get("runtime_identity_bound", ""),
            "identity_binding_source": item.get("identity_binding_source", ""),
            "identity_binding_frozen": item.get("identity_binding_frozen", ""),
        }
    return out


def _load_recommended_models(model_selection_dir: Path, component: str) -> Dict[str, Dict[str, str]]:
    rows = _read_csv(model_selection_dir / "model_selection_summary.csv")
    out: Dict[str, Dict[str, str]] = {}
    for row in rows:
        if str(row.get("component") or "").strip().lower() != component:
            continue
        if not _truthy(row.get("recommended_model")):
            continue
        device_id = str(row.get("analyzer_device_id") or "").strip()
        if device_id:
            out[device_id] = row
    return out


def _load_policy(candidate_dir: Path, component: str) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for row in _read_csv(candidate_dir / "candidate_policy_summary.csv"):
        if str(row.get("component") or "").strip().lower() != component:
            continue
        device_id = str(row.get("analyzer_device_id") or "").strip()
        if device_id:
            out[device_id] = row
    return out


def _load_candidate_summary(candidate_dir: Path) -> Dict[str, str]:
    rows = _read_csv(candidate_dir / "candidate_run_summary.csv")
    return dict(rows[0]) if len(rows) == 1 else {}


def _resolved_source(value: Any) -> str:
    text = str(value or "").strip()
    return str(Path(text).resolve()) if text else ""


def _fit_input_traceability_reasons(
    *,
    component: str,
    device_id: str,
    model: Optional[Mapping[str, Any]],
    policy: Optional[Mapping[str, Any]],
    candidate_summary: Mapping[str, Any],
) -> List[str]:
    details: List[str] = []
    component_key = str(component or "").strip().lower()
    normalized_device_id = str(device_id or "").strip()
    if not model:
        details.append("fit_input_traceability_model_selection_row_missing")
    if not policy:
        details.append("fit_input_traceability_candidate_policy_row_missing")
    if not candidate_summary:
        details.append("fit_input_traceability_candidate_run_summary_missing_or_ambiguous")

    if not _truthy(candidate_summary.get("fit_input_quality_required")):
        details.append("candidate_package_fit_input_quality_not_required")
    if str(candidate_summary.get("fit_input_quality_gate_status") or "").strip().lower() != "pass":
        details.append("candidate_package_fit_input_quality_gate_not_pass")

    for label, row in (("model_selection", model or {}), ("candidate_policy", policy or {})):
        grade = str(row.get("fit_input_quality_grade") or "").strip().upper()
        status = str(row.get("fit_input_quality_status") or "").strip().lower()
        if grade != "A":
            details.append(f"{label}_fit_input_quality_grade_not_a:{grade or 'missing'}")
        if status not in FIT_INPUT_READY_STATUSES:
            details.append(f"{label}_fit_input_quality_status_not_ready:{status or 'missing'}")
    if str((model or {}).get("fit_input_quality_gate_status") or "").strip().lower() != "pass":
        details.append("model_selection_fit_input_quality_gate_not_pass")
    model_block_reason = str((model or {}).get("fit_input_quality_block_reason") or "").strip()
    if model_block_reason:
        details.append(f"model_selection_fit_input_quality_blocked:{model_block_reason}")

    owners = {
        "candidate_summary": candidate_summary,
        "candidate_policy": policy or {},
        "model_selection": model or {},
    }
    bound_sources: Dict[str, Path] = {}
    for field in FIT_INPUT_SOURCE_FIELDS:
        resolved_by_owner: Dict[str, str] = {}
        for owner, row in owners.items():
            resolved = _resolved_source(row.get(field))
            if not resolved:
                details.append(f"{owner}_{field}_missing")
            else:
                resolved_by_owner[owner] = resolved
        unique_sources = set(resolved_by_owner.values())
        if len(unique_sources) > 1:
            details.append(f"{field}_mismatch_across_candidate_and_model_selection")
        elif len(unique_sources) == 1 and len(resolved_by_owner) == len(owners):
            source_path = Path(next(iter(unique_sources)))
            bound_sources[field] = source_path
            if not source_path.is_file():
                details.append(f"{field}_file_missing")

    summary_path = bound_sources.get("fit_input_quality_summary_source")
    if summary_path and summary_path.is_file():
        summary_rows = _read_csv(summary_path)
        if len(summary_rows) != 1:
            details.append(f"fit_input_quality_summary_row_count={len(summary_rows)}")
        else:
            summary = summary_rows[0]
            if str(summary.get("run_status") or "").strip().lower() != "pass":
                details.append("fit_input_quality_run_status_not_pass")
            if str(summary.get("fit_input_continuity_gate_status") or "").strip().lower() != "pass":
                details.append("fit_input_continuity_gate_not_pass")
            for field, reason in (
                ("opens_com_ports", "fit_input_quality_boundary_opens_com_ports"),
                ("controls_water_or_gas_routes", "fit_input_quality_boundary_controls_routes"),
                ("writes_coefficients", "fit_input_quality_boundary_writes_coefficients"),
            ):
                if _truthy(summary.get(field)):
                    details.append(reason)

    devices_path = bound_sources.get("fit_input_quality_devices_source")
    if devices_path and devices_path.is_file():
        group_rows = [
            row
            for row in _read_csv(devices_path)
            if str(row.get("component") or "").strip().lower() == component_key
            and str(row.get("analyzer_device_id") or row.get("device_id") or "").strip()
            == normalized_device_id
        ]
        if len(group_rows) != 1:
            details.append(f"fit_input_quality_device_group_row_count={len(group_rows)}")
        else:
            group = group_rows[0]
            grade = str(group.get("fit_input_grade") or "").strip().upper()
            status = str(group.get("fit_input_status") or "").strip().lower()
            if grade != "A":
                details.append(f"fit_input_quality_device_grade_not_a:{grade or 'missing'}")
            if status not in FIT_INPUT_READY_STATUSES:
                details.append(f"fit_input_quality_device_status_not_ready:{status or 'missing'}")

    deduped = list(dict.fromkeys(details))
    return [FIT_INPUT_TRACEABILITY_BLOCKER, *deduped] if deduped else []


def _snapshot_device(snapshot: Mapping[str, Any], device_id: str) -> Mapping[str, Any]:
    if not snapshot:
        return {}
    direct = snapshot.get(device_id)
    if isinstance(direct, Mapping):
        return direct
    devices = snapshot.get("devices")
    if isinstance(devices, Sequence) and not isinstance(devices, (str, bytes)):
        for item in devices:
            if not isinstance(item, Mapping):
                continue
            item_id = str(
                item.get("analyzer_device_id")
                or item.get("device_id")
                or item.get("runtime_device_id")
                or item.get("id")
                or ""
            ).strip()
            if item_id == device_id:
                return item
    return {}


def _snapshot_channel_value(device_snapshot: Mapping[str, Any], channel: int) -> Any:
    for key in (
        f"GETCO{channel}_before_live",
        f"GETCO{channel}_before_review",
        f"GETCO{channel}_before",
        f"GETCO{channel}",
        f"SENCO{channel}_before_live",
        f"SENCO{channel}_before_review",
        f"SENCO{channel}_before",
        f"SENCO{channel}",
        str(channel),
    ):
        value = device_snapshot.get(key)
        if value not in (None, ""):
            return value
    return ""


def _snapshot_has_values(value: Any, *, min_count: int = 4) -> bool:
    if value in (None, ""):
        return False
    candidate = value
    if isinstance(value, str):
        try:
            candidate = json.loads(value)
        except Exception:
            candidate = value
    if isinstance(candidate, Mapping):
        return len([key for key in candidate if str(key).upper().startswith("C")]) >= min_count
    if isinstance(candidate, Sequence) and not isinstance(candidate, (str, bytes)):
        return len(candidate) >= min_count
    return False


def _coef(row: Mapping[str, Any], term: str) -> float:
    value = _safe_float(row.get(f"coef_{term}"))
    return float(value) if value is not None else 0.0


def _payloads(row: Mapping[str, Any]) -> Tuple[List[float], List[float], List[str]]:
    primary = [_coef(row, term) for term in PRIMARY_TERMS] + [0.0, 0.0]
    secondary = [_coef(row, term) for term in SECONDARY_TERMS] + [0.0, 0.0, 0.0]
    nonzero_pressure: List[str] = []
    for term in PRESSURE_TERMS:
        value = _safe_float(row.get(f"coef_{term}"))
        if value is not None and abs(value) > 1e-12:
            nonzero_pressure.append(f"{term}={value}")
    return primary, secondary, nonzero_pressure


def _command(channel: int, values: Sequence[float]) -> str:
    return "SENCO{channel},YGAS,FFF,{payload}".format(
        channel=channel,
        payload=",".join(format_senco_values(values)),
    )


def _review_reasons(
    *,
    component: str,
    row: Optional[Mapping[str, Any]],
    policy: Optional[Mapping[str, Any]],
    include_devices: Sequence[str],
    device_id: str,
    max_relative_error_pct: float,
) -> List[str]:
    reasons: List[str] = []
    if device_id not in include_devices:
        reasons.append("not_in_current_write_scope")
    if row is None:
        reasons.append("missing_recommended_model")
    else:
        fit_status = str(row.get("fit_status") or "").strip()
        if fit_status != "ok":
            reasons.append(f"fit_status={fit_status or 'missing'}")
        rel = _safe_float(row.get("max_abs_relative_error_pct"))
        if rel is not None and rel > max_relative_error_pct:
            reasons.append(f"max_abs_relative_error_pct>{max_relative_error_pct:g}")
        gate = str(row.get("factory_signal_health_gate") or "").strip()
        if gate.startswith("block"):
            reasons.append(f"factory_signal_health_gate={gate}")
        note = str(row.get("review_note") or "").strip()
        if note.startswith("blocked"):
            reasons.append(note)
        _, _, nonzero_pressure = _payloads(row)
        if nonzero_pressure:
            reasons.append("pressure_terms_present_in_current_atmosphere_contract")
    if policy:
        blocked = str(policy.get("blocked_reasons") or "").strip()
        if blocked:
            reasons.append(blocked)
        status = str(policy.get("candidate_status") or "").strip()
        if status == "blocked":
            reasons.append("candidate_policy_status=blocked")
    return [reason for reason in reasons if reason]


def _old_snapshot_status(
    *,
    snapshot: Mapping[str, Any],
    device_id: str,
    primary_channel: int,
    secondary_channel: int,
    linear_channel: int,
) -> Tuple[str, Dict[str, Any]]:
    device_snapshot = _snapshot_device(snapshot, device_id)
    primary_old = _snapshot_channel_value(device_snapshot, primary_channel)
    secondary_old = _snapshot_channel_value(device_snapshot, secondary_channel)
    linear_old = _snapshot_channel_value(device_snapshot, linear_channel)
    primary_ok = _snapshot_has_values(primary_old, min_count=4)
    secondary_ok = _snapshot_has_values(secondary_old, min_count=3)
    linear_ok = _snapshot_has_values(linear_old, min_count=2)
    if primary_ok and secondary_ok and linear_ok:
        status = "bound_primary_secondary_linear"
    elif primary_ok and secondary_ok:
        status = "bound_primary_secondary_missing_linear"
    else:
        status = "missing_or_partial"
    return status, {
        "old_primary": primary_old,
        "old_secondary": secondary_old,
        "old_linear": linear_old,
        "old_primary_complete": primary_ok,
        "old_secondary_complete": secondary_ok,
        "old_linear_complete": linear_ok,
    }


def build_precheck_pack(
    *,
    co2_model_selection_dir: Path,
    h2o_model_selection_dir: Path,
    co2_candidate_dir: Path,
    h2o_candidate_dir: Path,
    output_dir: Path,
    plan_path: Optional[Path] = None,
    old_coefficients_path: Optional[Path] = None,
    include_devices: Sequence[str] = DEFAULT_INCLUDE_DEVICES,
    max_relative_error_pct: float = DEFAULT_RELATIVE_LIMIT_PCT,
) -> Dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    device_map = _load_device_map(plan_path)
    snapshot = _load_json(old_coefficients_path)
    include_devices = tuple(str(item).strip() for item in include_devices if str(item).strip())

    models = {
        "co2": _load_recommended_models(co2_model_selection_dir, "co2"),
        "h2o": _load_recommended_models(h2o_model_selection_dir, "h2o"),
    }
    policies = {
        "co2": _load_policy(co2_candidate_dir, "co2"),
        "h2o": _load_policy(h2o_candidate_dir, "h2o"),
    }
    candidate_summaries = {
        "co2": _load_candidate_summary(co2_candidate_dir),
        "h2o": _load_candidate_summary(h2o_candidate_dir),
    }
    all_devices = sorted(set(device_map) | set(models["co2"]) | set(models["h2o"]) | set(include_devices))

    summary_rows: List[Dict[str, Any]] = []
    command_rows: List[Dict[str, Any]] = []
    blocked_rows: List[Dict[str, Any]] = []
    neutral_rows: List[Dict[str, Any]] = []
    verification_rows: List[Dict[str, Any]] = []
    co2_mapping_rows: List[Dict[str, Any]] = []
    review_check_rows: List[Dict[str, Any]] = []
    h2o_payload_rows: List[Dict[str, Any]] = []
    h2o_policy_rows: List[Dict[str, Any]] = []
    h2o_diag_rows: List[Dict[str, Any]] = []
    fit_input_traceability_rows: List[Dict[str, Any]] = []

    for device_id in all_devices:
        map_row = device_map.get(device_id, {})
        device_ready = True
        component_statuses: Dict[str, str] = {}
        component_reasons: Dict[str, str] = {}
        component_models: Dict[str, str] = {}
        component_traceability_statuses: Dict[str, str] = {}
        component_traceability_reasons: Dict[str, str] = {}
        for component in ("co2", "h2o"):
            row = models[component].get(device_id)
            policy = policies[component].get(device_id)
            primary_channel, secondary_channel, linear_channel = _component_channels(component)
            traceability_reasons = _fit_input_traceability_reasons(
                component=component,
                device_id=device_id,
                model=row,
                policy=policy,
                candidate_summary=candidate_summaries[component],
            )
            reasons = _review_reasons(
                component=component,
                row=row,
                policy=policy,
                include_devices=include_devices,
                device_id=device_id,
                max_relative_error_pct=max_relative_error_pct,
            )
            reasons.extend(traceability_reasons)
            reasons = list(dict.fromkeys(reasons))
            traceability_status = "pass" if not traceability_reasons else "blocked"
            traceability_reason = ";".join(traceability_reasons)
            component_traceability_statuses[component] = traceability_status
            component_traceability_reasons[component] = traceability_reason
            fit_input_traceability_rows.append(
                {
                    "check": f"fit_input_traceability_bound:{component}:{device_id}",
                    "status": "pass" if traceability_status == "pass" else "block_write",
                    "component": component,
                    "analyzer_device_id": device_id,
                    "meaning": (
                        "Candidate summary, candidate policy, and model selection must bind the same live fit-input "
                        "quality summary/device artifacts before SENCO1/3, SENCO2/4, or S5/S6 review."
                    ),
                    "evidence": traceability_reason or str((row or {}).get("fit_input_quality_devices_source") or ""),
                }
            )
            old_status, old_detail = _old_snapshot_status(
                snapshot=snapshot,
                device_id=device_id,
                primary_channel=primary_channel,
                secondary_channel=secondary_channel,
                linear_channel=linear_channel,
            )
            model_status = "main_model_ready_for_write_review" if not reasons else "blocked_by_model_or_scope"
            write_gate_blockers = list(reasons)
            if old_status.startswith("missing"):
                write_gate_blockers.append("old_getco_snapshot_not_bound")
            elif old_status == "bound_primary_secondary_missing_linear":
                write_gate_blockers.append("old_s5_s6_snapshot_not_bound")
            if not write_gate_blockers:
                write_status = "ready_for_controlled_writer_after_review"
            elif model_status == "main_model_ready_for_write_review":
                write_status = "model_ready_but_write_blocked_until_snapshot_or_prerequisite"
            else:
                write_status = "blocked"
            if model_status != "main_model_ready_for_write_review":
                device_ready = False
            component_statuses[component] = model_status
            component_reasons[component] = ";".join(dict.fromkeys(reasons))
            component_models[component] = str(row.get("model_name") or "") if row else ""
            if row:
                primary_payload, secondary_payload, nonzero_pressure = _payloads(row)
                command_preview_allowed = not traceability_reasons
                if write_status == "ready_for_controlled_writer_after_review":
                    command_status = "pending_not_executed"
                elif model_status == "main_model_ready_for_write_review":
                    command_status = "pending_not_executed_requires_snapshot_or_prerequisite"
                else:
                    command_status = "blocked_not_executable"
                command_rows.extend(
                    [
                        {
                            "component": component,
                            "analyzer_prefix": row.get("analyzer_prefix") or map_row.get("analyzer_prefix", ""),
                            "analyzer_device_id": device_id,
                            "port_at_plan_snapshot": map_row.get("port", ""),
                            "senco_channel": f"SENCO{primary_channel}",
                            "command": _command(primary_channel, primary_payload) if command_preview_allowed else "",
                            "model_name": row.get("model_name", ""),
                            "status": command_status,
                            "model_blockers": component_reasons[component],
                            "write_gate_blockers": ";".join(dict.fromkeys(write_gate_blockers)),
                            "old_snapshot_status": old_status,
                            "fit_input_traceability_status": traceability_status,
                            "fit_input_traceability_blockers": traceability_reason,
                            "physical_meaning": (
                                "主光学比值到组分的低阶项；当前大气压合同下不包含压力项。"
                                if component == "co2"
                                else "主水汽比值到 H2O 的低阶项；当前大气压合同下不包含压力项。"
                            ),
                            "no_write": True,
                        },
                        {
                            "component": component,
                            "analyzer_prefix": row.get("analyzer_prefix") or map_row.get("analyzer_prefix", ""),
                            "analyzer_device_id": device_id,
                            "port_at_plan_snapshot": map_row.get("port", ""),
                            "senco_channel": f"SENCO{secondary_channel}",
                            "command": _command(secondary_channel, secondary_payload) if command_preview_allowed else "",
                            "model_name": row.get("model_name", ""),
                            "status": command_status,
                            "model_blockers": component_reasons[component],
                            "write_gate_blockers": ";".join(dict.fromkeys(write_gate_blockers)),
                            "old_snapshot_status": old_status,
                            "fit_input_traceability_status": traceability_status,
                            "fit_input_traceability_blockers": traceability_reason,
                            "physical_meaning": "温度相关补偿项；压力项保持为 0，由 SENCO9 单独负责。",
                            "no_write": True,
                        },
                    ]
                )
                if component == "co2":
                    co2_mapping_rows.append(
                        {
                            "component": "co2",
                            "analyzer_prefix": row.get("analyzer_prefix") or map_row.get("analyzer_prefix", ""),
                            "analyzer_device_id": device_id,
                            "primary_senco": "SENCO1",
                            "secondary_senco": "SENCO3",
                            "candidate_terms": "intercept;R;R2;R3;T;T2;RT",
                            "candidate_terms_complete": model_status == "main_model_ready_for_write_review",
                            "secondary_candidate_terms_complete": model_status == "main_model_ready_for_write_review",
                            "primary_candidate_values": json.dumps(primary_payload, ensure_ascii=False),
                            "secondary_candidate_values": json.dumps(secondary_payload, ensure_ascii=False),
                            "primary_command_preview": _command(1, primary_payload) if command_preview_allowed else "",
                            "secondary_command_preview": _command(3, secondary_payload) if command_preview_allowed else "",
                            "old_primary_snapshot": json.dumps(old_detail["old_primary"], ensure_ascii=False, default=str),
                            "old_secondary_snapshot": json.dumps(old_detail["old_secondary"], ensure_ascii=False, default=str),
                            "old_snapshot_status": (
                                "primary_and_secondary_bound"
                                if old_status in {"bound_primary_secondary_linear", "bound_primary_secondary_missing_linear"}
                                else "partial_or_missing"
                            ),
                            "mapping_status": (
                                "review_only_primary_secondary_preview_ready"
                                if model_status == "main_model_ready_for_write_review"
                                else (
                                    "blocked_fit_input_traceability"
                                    if traceability_reasons
                                    else "blocked"
                                )
                            ),
                            "write_allowed": False,
                            "model_name": row.get("model_name", ""),
                            "model_blockers": component_reasons[component],
                            "write_gate_blockers": ";".join(dict.fromkeys(write_gate_blockers)),
                            "fit_input_traceability_status": traceability_status,
                            "fit_input_traceability_blockers": traceability_reason,
                        }
                    )
                elif component == "h2o":
                    candidate_status = (
                        "candidate_fit_ready_main_chain_precheck"
                        if model_status == "main_model_ready_for_write_review"
                        else "blocked"
                    )
                    h2o_payload_rows.append(
                        {
                            "component": "h2o",
                            "analyzer_prefix": row.get("analyzer_prefix") or map_row.get("analyzer_prefix", ""),
                            "analyzer_device_id": device_id,
                            "primary_senco": "SENCO2",
                            "secondary_senco": "SENCO4",
                            "senco2_payload_values_json": json.dumps(primary_payload, ensure_ascii=False),
                            "senco4_payload_values_json": json.dumps(secondary_payload, ensure_ascii=False),
                            "senco2_command_preview": _command(2, primary_payload) if command_preview_allowed else "",
                            "senco4_command_preview": _command(4, secondary_payload) if command_preview_allowed else "",
                            "model_name": row.get("model_name", ""),
                            "candidate_status": candidate_status,
                            "write_gate_blockers": ";".join(dict.fromkeys(write_gate_blockers)),
                            "fit_input_traceability_status": traceability_status,
                            "fit_input_traceability_blockers": traceability_reason,
                        }
                    )
                    h2o_policy_rows.append(
                        {
                            "component": "h2o",
                            "analyzer_prefix": row.get("analyzer_prefix") or map_row.get("analyzer_prefix", ""),
                            "analyzer_device_id": device_id,
                            "candidate_status": candidate_status,
                            "blocked_reasons": component_reasons[component],
                            "warning_reasons": "SENCO6_output_layer_is_separate_after_main_chain",
                            "old_snapshot_status": old_status,
                            "model_name": row.get("model_name", ""),
                            "fit_input_traceability_status": traceability_status,
                            "fit_input_traceability_blockers": traceability_reason,
                        }
                    )
                    h2o_diag_rows.append(
                        {
                            "component": "h2o",
                            "analyzer_prefix": row.get("analyzer_prefix") or map_row.get("analyzer_prefix", ""),
                            "analyzer_device_id": device_id,
                            "diagnosis": "main_chain_precheck_only_senco6_separate_layer",
                            "model_name": row.get("model_name", ""),
                        }
                    )
                if nonzero_pressure:
                    blocked_rows.append(
                        {
                            "component": component,
                            "analyzer_device_id": device_id,
                            "blocker": "pressure_terms_present",
                            "detail": ";".join(nonzero_pressure),
                        }
                    )
            if write_gate_blockers:
                blocked_rows.append(
                    {
                        "component": component,
                        "analyzer_prefix": (row or policy or map_row).get("analyzer_prefix", ""),
                        "analyzer_device_id": device_id,
                        "model_name": str(row.get("model_name") or "") if row else "",
                        "model_blocker": ";".join(dict.fromkeys(reasons)),
                        "write_gate_blocker": ";".join(dict.fromkeys(write_gate_blockers)),
                        "max_abs_error": str(row.get("max_abs_error") or "") if row else "",
                        "max_abs_relative_error_pct": str(row.get("max_abs_relative_error_pct") or "") if row else "",
                        "factory_signal_health_gate": str(row.get("factory_signal_health_gate") or "") if row else "",
                        "old_snapshot_status": old_status,
                    }
                )
            neutral_rows.append(
                {
                    "component": component,
                    "analyzer_prefix": (row or policy or map_row).get("analyzer_prefix", ""),
                    "analyzer_device_id": device_id,
                    "linear_senco_channel": f"SENCO{linear_channel}",
                    "required_state_before_main_verification": "C0=0,C1=1",
                    "recommended_command_if_approved": (
                        f"SENCO{linear_channel},YGAS,FFF,0,1" if not reasons else ""
                    ),
                    "reason": (
                        "S5/S6 是最终显示层线性修正。主链路复验前中性化，避免把输出层偏差混进 S1/3 或 S2/4。"
                    ),
                    "old_snapshot_status": old_status,
                    "status": (
                        "prerequisite_only_not_executed"
                        if not reasons
                        else "blocked_before_s5_s6_write_review"
                    ),
                    "fit_input_traceability_status": traceability_status,
                    "fit_input_traceability_blockers": traceability_reason,
                }
            )
        summary_rows.append(
            {
                "analyzer_device_id": device_id,
                "analyzer_prefix": map_row.get("analyzer_prefix", ""),
                "port_at_plan_snapshot": map_row.get("port", ""),
                "configured_device_id": map_row.get("configured_device_id", ""),
                "runtime_identity_bound": map_row.get("runtime_identity_bound", ""),
                "co2_model": component_models.get("co2", ""),
                "h2o_model": component_models.get("h2o", ""),
                "co2_model_status": component_statuses.get("co2", "missing"),
                "h2o_model_status": component_statuses.get("h2o", "missing"),
                "co2_model_blockers": component_reasons.get("co2", ""),
                "h2o_model_blockers": component_reasons.get("h2o", ""),
                "co2_fit_input_traceability_status": component_traceability_statuses.get("co2", "blocked"),
                "h2o_fit_input_traceability_status": component_traceability_statuses.get("h2o", "blocked"),
                "co2_fit_input_traceability_blockers": component_traceability_reasons.get("co2", ""),
                "h2o_fit_input_traceability_blockers": component_traceability_reasons.get("h2o", ""),
                "overall_status": (
                    "model_ready_for_main_senco_review"
                    if device_ready
                    else "blocked_before_write_review"
                ),
                "no_write": True,
            }
        )
        if device_id in include_devices:
            verification_rows.extend(
                [
                    {
                        "analyzer_device_id": device_id,
                        "component": "co2",
                        "phase": "after_S1_S3_main_write_before_S5",
                        "verification_scope": "开放流通，S5 中性；优先 100/400/800 或 900 ppm 非零点，必要时含低端锚点。",
                        "acceptance_note": "先判断主链路残差和物理状态，再决定是否进入 S5 输出层修正。",
                    },
                    {
                        "analyzer_device_id": device_id,
                        "component": "h2o",
                        "phase": "after_S2_S4_main_write_before_S6",
                        "verification_scope": "开放流通，S6 中性；选择低/中/高水汽点，保留露点、dry/wet ppmv、温度证据。",
                        "acceptance_note": "先判断水汽主链路，再决定是否进入 S6 输出层修正。",
                    },
                ]
            )

    paths = {
        "summary": output_dir / "main_senco_write_precheck_summary.csv",
        "commands": output_dir / "main_senco_write_commands_pending.csv",
        "blocked": output_dir / "main_senco_blocked_devices.csv",
        "neutral": output_dir / "main_senco_s5_s6_neutral_prerequisites.csv",
        "verification": output_dir / "main_senco_post_main_verification_plan.csv",
        "co2_mapping": output_dir / "candidate_senco_mapping_review.csv",
        "write_checks": output_dir / "candidate_write_review_checks.csv",
        "h2o_payload": output_dir / "h2o_senco24_payload_preview.csv",
        "h2o_policy": output_dir / "h2o_senco24_device_policy.csv",
        "h2o_diagnostics": output_dir / "h2o_senco24_output_diagnostics.csv",
        "report": output_dir / "main_senco_write_precheck_pack_zh.md",
        "hash_manifest": output_dir / "main_senco_artifact_hash_manifest.json",
        "meta": output_dir / "main_senco_write_precheck_meta.json",
    }
    _write_csv(paths["summary"], summary_rows)
    _write_csv(paths["commands"], command_rows)
    _write_csv(paths["blocked"], blocked_rows)
    _write_csv(paths["neutral"], neutral_rows)
    _write_csv(paths["verification"], verification_rows)
    _write_csv(paths["co2_mapping"], co2_mapping_rows)
    scoped_traceability_rows = [
        row for row in fit_input_traceability_rows if row.get("analyzer_device_id") in include_devices
    ]
    fit_input_traceability_status = (
        "pass"
        if scoped_traceability_rows
        and all(row.get("status") == "pass" for row in scoped_traceability_rows)
        else "blocked"
    )
    review_check_rows.extend(
        [
            {
                "check": "fit_input_traceability_required_before_final_senco_review",
                "status": "pass" if fit_input_traceability_status == "pass" else "block_write",
                "meaning": "Final SENCO1/3, SENCO2/4, and S5/S6 review rejects historical candidate packages without bound fit-input evidence.",
                "evidence": "per-device component checks follow",
            },
            {
                "check": "firmware_formula_contract_confirmed",
                "status": "pass",
                "meaning": "SENCO1/3 and SENCO2/4 main-chain formula reviewed as ratio/temperature terms; pressure remains separate SENCO9 input.",
                "evidence": "manual_senco13_rt_pressure_separate_v1_5",
            },
            {
                "check": "co2_senco5_senco6_linear_correction_contract",
                "status": "pass",
                "meaning": "S5/S6 are separate final affine output layers and are not folded into main-chain coefficients.",
                "evidence": "main_senco_s5_s6_neutral_prerequisites.csv",
            },
        ]
    )
    review_check_rows.extend(fit_input_traceability_rows)
    _write_csv(paths["write_checks"], review_check_rows)
    _write_csv(paths["h2o_payload"], h2o_payload_rows)
    _write_csv(paths["h2o_policy"], h2o_policy_rows)
    _write_csv(paths["h2o_diagnostics"], h2o_diag_rows)

    ready = [row for row in summary_rows if row["overall_status"].startswith("model_ready")]
    blocked = [row for row in summary_rows if row["overall_status"].startswith("blocked")]
    report_lines = [
        "# V1.5 主系数写入前评审包",
        "",
        f"- 生成时间：{_now()}",
        f"- 输出目录：`{output_dir}`",
        "- 性质：离线 no-write 评审包；未打开 COM、未写 SENCO、未控制气路/水路/压力。",
        "- 主链路：CO2 使用 SENCO1/SENCO3，H2O 使用 SENCO2/SENCO4。",
        "- 输出层：S5/S6 不在本阶段写入；主链路复验前应中性化为 C0=0, C1=1，或显式建模旧值。",
        "- 溯源门禁：S1/3、S2/4、S5/S6 写前评审必须绑定同一份通过的 fit-input 质量摘要和逐设备证据；历史无溯源候选直接阻断。",
        "- 压力：当前大气压开放流通主拟合不写压力项；压力输入由 SENCO9 独立校准与验证。",
        "- 锚点：CO2 零气低端锚点与 H2O 干气低端锚点物理意义不同，不能互相替代。",
        "",
        "## 可进入受控写入评审的设备",
    ]
    if ready:
        for row in ready:
            report_lines.append(
                f"- ID {row['analyzer_device_id']}：CO2 `{row['co2_model']}`，H2O `{row['h2o_model']}`。"
            )
    else:
        report_lines.append("- 暂无。")
    report_lines.extend(["", "## 写入前阻断"])
    if blocked:
        for row in blocked:
            report_lines.append(
                f"- ID {row['analyzer_device_id']}：CO2 `{row['co2_model_blockers']}`；H2O `{row['h2o_model_blockers']}`。"
            )
    else:
        report_lines.append("- 无。")
    report_lines.extend(
        [
            "",
            "## 下一步",
            "",
            "1. 先绑定当前设备 ID 与旧 GETCO1-9 快照。",
            "2. 受控中性化 S5/S6 后写主系数 S1/S3、S2/S4。",
            "3. 主链路开放流通复验通过后，再单独评审 S5/S6 是否能进一步压低最终显示误差。",
        ]
    )
    paths["report"].write_text("\n".join(report_lines) + "\n", encoding="utf-8")

    hash_artifacts: Dict[str, Path] = {
        "co2_candidate_run_summary": co2_candidate_dir / "candidate_run_summary.csv",
        "co2_candidate_policy_summary": co2_candidate_dir / "candidate_policy_summary.csv",
        "co2_model_selection_summary": co2_model_selection_dir / "model_selection_summary.csv",
        "h2o_candidate_run_summary": h2o_candidate_dir / "candidate_run_summary.csv",
        "h2o_candidate_policy_summary": h2o_candidate_dir / "candidate_policy_summary.csv",
        "h2o_model_selection_summary": h2o_model_selection_dir / "model_selection_summary.csv",
        "precheck_summary": paths["summary"],
        "precheck_commands": paths["commands"],
        "precheck_blocked": paths["blocked"],
        "precheck_neutral_prerequisites": paths["neutral"],
        "precheck_verification_plan": paths["verification"],
        "precheck_co2_mapping": paths["co2_mapping"],
        "precheck_checks": paths["write_checks"],
        "precheck_h2o_payload": paths["h2o_payload"],
        "precheck_h2o_policy": paths["h2o_policy"],
        "precheck_h2o_diagnostics": paths["h2o_diagnostics"],
    }
    for component, summary in candidate_summaries.items():
        for field, suffix in (
            ("fit_input_quality_summary_source", "fit_input_quality_summary"),
            ("fit_input_quality_devices_source", "fit_input_quality_devices"),
        ):
            source = str(summary.get(field) or "").strip()
            if source:
                hash_artifacts[f"{component}_{suffix}"] = Path(source)
    if plan_path:
        hash_artifacts["runtime_plan"] = plan_path
    if old_coefficients_path:
        hash_artifacts["old_coefficients_snapshot"] = old_coefficients_path
    write_artifact_hash_manifest(paths["hash_manifest"], artifacts=hash_artifacts)

    meta = {
        "generated_at": _now(),
        "no_write": True,
        "opens_com": False,
        "writes_senco": False,
        "controls_routes": False,
        "include_devices": list(include_devices),
        "max_relative_error_pct": max_relative_error_pct,
        "plan_path": str(plan_path) if plan_path else "",
        "old_coefficients_path": str(old_coefficients_path) if old_coefficients_path else "",
        "ready_device_count": len(ready),
        "blocked_device_count": len(blocked),
        "fit_input_traceability_required": True,
        "fit_input_traceability_status": fit_input_traceability_status,
        "artifact_hash_manifest_required": True,
        "artifact_hash_manifest_path": str(paths["hash_manifest"].resolve()),
        "artifact_hash_algorithm": "sha256",
        "outputs": {key: str(value) for key, value in paths.items()},
    }
    paths["meta"].write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return paths


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export V1.5 main SENCO no-write precheck package.")
    parser.add_argument("--co2-model-selection-dir", required=True)
    parser.add_argument("--h2o-model-selection-dir", required=True)
    parser.add_argument("--co2-candidate-dir", required=True)
    parser.add_argument("--h2o-candidate-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--plan-json", default=None)
    parser.add_argument("--old-coefficients-json", default=None)
    parser.add_argument("--include-device-id", action="append", default=None)
    parser.add_argument("--max-relative-error-pct", type=float, default=DEFAULT_RELATIVE_LIMIT_PCT)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        paths = build_precheck_pack(
            co2_model_selection_dir=Path(args.co2_model_selection_dir),
            h2o_model_selection_dir=Path(args.h2o_model_selection_dir),
            co2_candidate_dir=Path(args.co2_candidate_dir),
            h2o_candidate_dir=Path(args.h2o_candidate_dir),
            output_dir=Path(args.output_dir),
            plan_path=Path(args.plan_json) if args.plan_json else None,
            old_coefficients_path=Path(args.old_coefficients_json) if args.old_coefficients_json else None,
            include_devices=tuple(args.include_device_id or DEFAULT_INCLUDE_DEVICES),
            max_relative_error_pct=float(args.max_relative_error_pct),
        )
    except Exception as exc:
        print(f"V1.5 main SENCO precheck export failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(json.dumps({key: str(value.resolve()) for key, value in paths.items()}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
