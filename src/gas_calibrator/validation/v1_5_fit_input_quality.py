"""V1.5 offline fit-input quality audit.

This module reads existing no-write CO2/H2O candidate review artifacts and
states which analyzer-device inputs are allowed to feed coefficient fitting.
It never opens COM ports, controls routes, or writes coefficients.
"""

from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


@dataclass(frozen=True)
class FitInputQualityConfig:
    target_device_ids: Tuple[str, ...] = ("022", "030", "033", "051")
    excluded_device_ids: Tuple[str, ...] = ("023", "100")
    co2_min_fit_samples: int = 10
    h2o_min_complete_points: int = 8
    h2o_min_wet_points: int = 3


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _device_id(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.startswith("GA"):
        text = text[2:]
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "pass", "ok"}


def _safe_int(value: Any) -> int:
    try:
        number = float(value)
    except Exception:
        return 0
    if not math.isfinite(number):
        return 0
    return int(number)


def _read_csv(path: str | Path | None) -> List[Dict[str, Any]]:
    if not path:
        return []
    source = Path(path)
    if not source.exists():
        return []
    with source.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _read_json(path: str | Path | None) -> Dict[str, Any]:
    if not path:
        return {}
    source = Path(path)
    if not source.exists():
        return {}
    payload = json.loads(source.read_text(encoding="utf-8-sig"))
    return dict(payload) if isinstance(payload, Mapping) else {}


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


def _write_json(path: Path, value: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return path


def _rows_by_device(rows: Sequence[Mapping[str, Any]]) -> Dict[str, List[Mapping[str, Any]]]:
    grouped: Dict[str, List[Mapping[str, Any]]] = {}
    for row in rows:
        device = _device_id(row.get("analyzer_device_id") or row.get("device_id") or row.get("device"))
        if device:
            grouped.setdefault(device, []).append(row)
    return grouped


def _single_by_device(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Mapping[str, Any]]:
    out: Dict[str, Mapping[str, Any]] = {}
    for row in rows:
        device = _device_id(row.get("analyzer_device_id") or row.get("device_id") or row.get("device"))
        if device:
            out[device] = row
    return out


def _excluded_rows(
    *,
    component: str,
    rows: Sequence[Mapping[str, Any]],
    excluded_devices: set[str],
    source_table: str,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        device = _device_id(row.get("analyzer_device_id") or row.get("device_id") or row.get("device"))
        if device not in excluded_devices:
            continue
        out.append(
            {
                "component": component,
                "device_id": device,
                "source_table": source_table,
                "point_identity": row.get("point_identity") or row.get("point_run_id") or row.get("point_id") or "",
                "sample_role": row.get("sample_role") or row.get("residual_role") or "",
                "exclude_reason": "device_excluded_from_current_calibration_scope",
                "fit_input_grade": "EXCLUDED",
                "physical_meaning": (
                    "This analyzer evidence is preserved for diagnosis only and is not allowed to influence "
                    "the current coefficient candidate."
                ),
            }
        )
    return out


def _excluded_scope_placeholders(
    *,
    devices: set[str],
    existing_rows: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    recorded = {_device_id(row.get("device_id")) for row in existing_rows}
    out: List[Dict[str, Any]] = []
    for device in sorted(devices):
        if device in recorded:
            continue
        out.append(
            {
                "component": "scope",
                "device_id": device,
                "source_table": "scope_exclusion",
                "point_identity": "",
                "sample_role": "",
                "exclude_reason": "device_excluded_from_current_calibration_scope_no_source_rows_found",
                "fit_input_grade": "EXCLUDED",
                "physical_meaning": (
                    "This analyzer is intentionally outside the current calibration scope. No source row was "
                    "present in the selected candidate artifacts, so this placeholder prevents the exclusion "
                    "from becoming invisible in the audit trail."
                ),
            }
        )
    return out


def _formal_status_continuity_ready(payload: Mapping[str, Any]) -> Tuple[bool, str]:
    if not payload:
        return False, "formal_run_status_missing"
    for gate in payload.get("gates") or []:
        if not isinstance(gate, Mapping):
            continue
        if str(gate.get("gate_id") or "") != "mature_route_continuity_gate":
            continue
        status = str(gate.get("status") or "").strip().lower()
        if status == "ready":
            return True, "formal_run_status_mature_route_continuity_ready"
        return False, f"formal_run_status_mature_route_continuity_{status or 'missing'}"
    return False, "formal_run_status_mature_route_continuity_gate_missing"


def _direct_continuity_gate_ready(payload: Mapping[str, Any]) -> Tuple[bool, str]:
    if not payload:
        return False, "mature_route_continuity_gate_missing"
    manifest = payload.get("manifest") if isinstance(payload.get("manifest"), Mapping) else payload
    status = str(manifest.get("status") or "").strip().lower()
    fit_eligible = manifest.get("continuous_route_run_fit_eligible") is True
    blocker_count = _safe_int(manifest.get("blocker_count"))
    review_count = _safe_int(manifest.get("review_required_count"))
    if status == "pass" and fit_eligible and blocker_count == 0 and review_count == 0:
        return True, "mature_route_continuity_gate_passed"
    return (
        False,
        "mature_route_continuity_gate_not_ready:"
        f"status={status or 'missing'};fit_eligible={fit_eligible};"
        f"blocker_count={blocker_count};review_required_count={review_count}",
    )


def _continuity_consumer_state(
    *,
    formal_run_status_json: str | Path | None,
    mature_route_continuity_gate_json: str | Path | None,
) -> Dict[str, Any]:
    formal_payload = _read_json(formal_run_status_json)
    continuity_payload = _read_json(mature_route_continuity_gate_json)
    reasons: List[str] = []
    ready = False
    formal_ready = False
    direct_ready = False
    if formal_run_status_json:
        formal_ready, reason = _formal_status_continuity_ready(formal_payload)
        reasons.append(reason)
        ready = ready or formal_ready
    if mature_route_continuity_gate_json:
        direct_ready, reason = _direct_continuity_gate_ready(continuity_payload)
        reasons.append(reason)
        ready = ready or direct_ready
    if not formal_run_status_json and not mature_route_continuity_gate_json:
        reasons.append("fit_input_continuity_evidence_missing")
    return {
        "ready": ready,
        "formal_run_status_ready": formal_ready,
        "direct_continuity_gate_ready": direct_ready,
        "reason": ";".join(dict.fromkeys(reasons)),
        "formal_run_status_json": str(Path(formal_run_status_json).resolve()) if formal_run_status_json else "",
        "mature_route_continuity_gate_json": (
            str(Path(mature_route_continuity_gate_json).resolve()) if mature_route_continuity_gate_json else ""
        ),
    }


def _profile_lineage_state(path: str | Path | None) -> Dict[str, Any]:
    if not path:
        return {
            "configured": False,
            "ready": True,
            "reason": "algorithm_profile_lineage_not_configured_legacy_compatibility",
            "source": "",
            "contract": {},
        }
    source = Path(path).resolve()
    payload = _read_json(source)
    contract = payload.get("fit_input_contract") if isinstance(payload.get("fit_input_contract"), Mapping) else {}
    reasons: List[str] = []
    if payload.get("overall_status") != "pass":
        reasons.append("algorithm_profile_lineage_status_not_pass")
    if payload.get("fit_input_allowed") is not True:
        reasons.append("algorithm_profile_lineage_fit_input_not_allowed")
    if not contract.get("profile_id") or not contract.get("profile_sha256"):
        reasons.append("algorithm_profile_identity_missing")
    if not contract.get("co2_fit_input") or not contract.get("h2o_fit_input"):
        reasons.append("algorithm_profile_fit_variables_missing")
    for key in ("opens_com_ports", "controls_water_or_gas_routes", "writes_coefficients", "connects_postgresql"):
        if payload.get(key) is not False:
            reasons.append(f"algorithm_profile_lineage_{key}_must_be_false")
    return {
        "configured": True,
        "ready": not reasons,
        "reason": ";".join(reasons) if reasons else "algorithm_profile_lineage_passed",
        "source": str(source),
        "contract": dict(contract),
    }


def _apply_continuity_block(device_rows: List[Dict[str, Any]], reason: str) -> None:
    for row in device_rows:
        row["fit_input_grade"] = "REJECT"
        row["fit_input_status"] = "excluded_from_candidate_fit"
        existing = str(row.get("reject_reasons") or "").strip()
        parts = [part for part in existing.split(";") if part]
        parts.append(f"fit_input_continuity_gate_not_ready:{reason}")
        row["reject_reasons"] = ";".join(dict.fromkeys(parts))


def _co2_device_quality(
    *,
    device: str,
    policy: Mapping[str, Any],
    residual_count: int,
    cfg: FitInputQualityConfig,
) -> Dict[str, Any]:
    reasons: List[str] = []
    warnings: List[str] = []
    if not policy:
        reasons.append("co2_policy_missing")
    if policy and not _truthy(policy.get("allowed_to_fit")):
        reasons.append("co2_policy_not_allowed_to_fit")
    fit_samples = _safe_int(policy.get("fit_sample_count"))
    if fit_samples < int(cfg.co2_min_fit_samples):
        reasons.append(f"co2_fit_samples<{int(cfg.co2_min_fit_samples)}")
    if residual_count <= 0:
        reasons.append("co2_fit_residual_points_missing")
    if _safe_int(policy.get("preparation_rejected_count")) > 0:
        reasons.append("co2_preparation_rejected_points_present")
    if _safe_int(policy.get("formal_a_grade_count")) < fit_samples:
        reasons.append("co2_formal_a_grade_count_less_than_fit_samples")
    blocked = str(policy.get("blocked_reasons") or "").strip()
    if blocked:
        reasons.extend(f"co2_blocked:{item}" for item in blocked.split(";") if item)
    warning_text = str(policy.get("warning_reasons") or "").strip()
    if warning_text:
        warnings.extend(item for item in warning_text.split(";") if item)
    return {
        "component": "co2",
        "device_id": device,
        "fit_input_grade": "A" if not reasons else "REJECT",
        "fit_input_status": "usable_for_candidate_fit" if not reasons else "excluded_from_candidate_fit",
        "fit_sample_count": fit_samples,
        "fit_point_count": _safe_int(policy.get("fit_point_count")) or residual_count,
        "candidate_status": policy.get("candidate_status", ""),
        "formal_a_grade_count": _safe_int(policy.get("formal_a_grade_count")),
        "preparation_rejected_count": _safe_int(policy.get("preparation_rejected_count")),
        "residual_point_count": residual_count,
        "reject_reasons": ";".join(dict.fromkeys(reasons)),
        "warning_reasons": ";".join(dict.fromkeys(warnings)),
        "quality_scope": "open_flow_current_atmosphere_CO2_ratio_fit_input",
        "physical_meaning": (
            "CO2 fitting input is accepted only when it came from A-grade open-flow samples and no "
            "preparation reject was needed for this analyzer."
        ),
    }


def _h2o_device_quality(
    *,
    device: str,
    policy: Mapping[str, Any],
    residual_count: int,
    cfg: FitInputQualityConfig,
) -> Dict[str, Any]:
    reasons: List[str] = []
    warnings: List[str] = []
    if not policy:
        reasons.append("h2o_policy_missing")
    complete = _safe_int(policy.get("complete_point_count"))
    wet = _safe_int(policy.get("complete_wet_point_count"))
    dry = _safe_int(policy.get("complete_dry_anchor_count"))
    if complete < int(cfg.h2o_min_complete_points):
        reasons.append(f"h2o_complete_points<{int(cfg.h2o_min_complete_points)}")
    if wet < int(cfg.h2o_min_wet_points):
        reasons.append(f"h2o_wet_points<{int(cfg.h2o_min_wet_points)}")
    if residual_count <= 0:
        reasons.append("h2o_fit_residual_points_missing")
    if _safe_int(policy.get("rejected_point_count")) > 0:
        reasons.append("h2o_rejected_points_present")
    blocked = str(policy.get("blocked_reasons") or "").strip()
    if blocked:
        reasons.extend(f"h2o_blocked:{item}" for item in blocked.split(";") if item)
    warning_text = str(policy.get("warning_reasons") or "").strip()
    if warning_text:
        warnings.extend(item for item in warning_text.split(";") if item)
    fit_design_qc = str(policy.get("fit_design_qc") or "").strip()
    if fit_design_qc and fit_design_qc != "pass":
        warnings.append(f"h2o_model_fit_qc_{fit_design_qc}_not_input_quality_reject")
    return {
        "component": "h2o",
        "device_id": device,
        "fit_input_grade": "A" if not reasons else "REJECT",
        "fit_input_status": "usable_for_candidate_fit" if not reasons else "excluded_from_candidate_fit",
        "fit_sample_count": complete,
        "fit_point_count": complete,
        "complete_wet_point_count": wet,
        "complete_dry_anchor_count": dry,
        "candidate_status": policy.get("candidate_status", ""),
        "fit_design_qc": fit_design_qc,
        "fit_max_abs_relative_error_pct": policy.get("fit_max_abs_relative_error_pct", ""),
        "rejected_point_count": _safe_int(policy.get("rejected_point_count")),
        "residual_point_count": residual_count,
        "reject_reasons": ";".join(dict.fromkeys(reasons)),
        "warning_reasons": ";".join(dict.fromkeys(warnings)),
        "quality_scope": "open_flow_H2O_ratio_temperature_fit_input",
        "physical_meaning": (
            "H2O fitting input is accepted when wet evidence and dry-gas anchors are complete and no "
            "device-level block exists. Model residual review is tracked separately from input cleanliness."
        ),
    }


def build_fit_input_quality_tables(
    *,
    co2_policy_csv: str | Path,
    co2_residuals_csv: str | Path,
    h2o_policy_csv: str | Path,
    h2o_residuals_csv: str | Path,
    h2o_point_inputs_csv: str | Path | None = None,
    formal_run_status_json: str | Path | None = None,
    mature_route_continuity_gate_json: str | Path | None = None,
    algorithm_profile_lineage_json: str | Path | None = None,
    cfg: FitInputQualityConfig = FitInputQualityConfig(),
) -> Dict[str, List[Dict[str, Any]]]:
    target_devices = {_device_id(item) for item in cfg.target_device_ids}
    excluded_devices = {_device_id(item) for item in cfg.excluded_device_ids}
    co2_policies = _single_by_device(_read_csv(co2_policy_csv))
    co2_residuals = _read_csv(co2_residuals_csv)
    h2o_policies = _single_by_device(_read_csv(h2o_policy_csv))
    h2o_residuals = _read_csv(h2o_residuals_csv)
    h2o_point_inputs = _read_csv(h2o_point_inputs_csv)
    continuity_state = _continuity_consumer_state(
        formal_run_status_json=formal_run_status_json,
        mature_route_continuity_gate_json=mature_route_continuity_gate_json,
    )
    lineage_state = _profile_lineage_state(algorithm_profile_lineage_json)

    co2_residuals_by_device = _rows_by_device(co2_residuals)
    h2o_residuals_by_device = _rows_by_device(h2o_residuals)

    device_rows: List[Dict[str, Any]] = []
    for device in sorted(target_devices):
        device_rows.append(
            _co2_device_quality(
                device=device,
                policy=co2_policies.get(device, {}),
                residual_count=len(co2_residuals_by_device.get(device, [])),
                cfg=cfg,
            )
        )
        device_rows.append(
            _h2o_device_quality(
                device=device,
                policy=h2o_policies.get(device, {}),
                residual_count=len(h2o_residuals_by_device.get(device, [])),
                cfg=cfg,
            )
        )

    input_chain_ready = bool(continuity_state["ready"] and lineage_state["ready"])
    if not input_chain_ready:
        _apply_continuity_block(
            device_rows,
            ";".join(
                part
                for part in (
                    str(continuity_state["reason"]) if not continuity_state["ready"] else "",
                    str(lineage_state["reason"]) if not lineage_state["ready"] else "",
                )
                if part
            ),
        )

    point_rows: List[Dict[str, Any]] = []
    lineage_contract = lineage_state.get("contract") or {}
    quality_by_component_device = {
        (row["component"], row["device_id"]): row["fit_input_grade"] for row in device_rows
    }
    for component, rows in (("co2", co2_residuals), ("h2o", h2o_residuals)):
        for row in rows:
            device = _device_id(row.get("analyzer_device_id") or row.get("device_id") or row.get("device"))
            if device not in target_devices:
                continue
            grade = quality_by_component_device.get((component, device), "REJECT")
            point_rows.append(
                {
                    "component": component,
                    "device_id": device,
                    "point_identity": row.get("point_identity") or row.get("point_run_id") or row.get("point_id") or "",
                    "sample_role": row.get("sample_role") or row.get("residual_role") or "",
                    "target_value": row.get("target_value") or row.get("reference_h2o_mmol") or "",
                    "ratio": row.get("ratio") or row.get("h2o_ratio_f") or "",
                    "temperature_c": row.get("temperature_c") or row.get("chamber_temp_c") or "",
                    "algorithm_profile_id": lineage_contract.get("profile_id", ""),
                    "algorithm_profile_sha256": lineage_contract.get("profile_sha256", ""),
                    "fit_input_variable": lineage_contract.get(f"{component}_fit_input", ""),
                    "fit_input_grade": grade,
                    "model_error": row.get("error") or row.get("model_error_mmol") or "",
                    "model_error_pct": row.get("model_error_pct") or "",
                    "source_quality_basis": (
                        "candidate_residual_from_a_grade_open_flow_samples"
                        if grade == "A"
                        else "candidate_residual_not_allowed_for_fit"
                    ),
                    "physical_meaning": (
                        "This row may feed the coefficient model only if its device/component grade is A."
                    ),
                }
            )

    excluded_rows: List[Dict[str, Any]] = []
    excluded_rows.extend(
        _excluded_rows(
            component="co2",
            rows=list(co2_policies.values()) + co2_residuals,
            excluded_devices=excluded_devices,
            source_table="co2_policy_or_residual",
        )
    )
    excluded_rows.extend(
        _excluded_rows(
            component="h2o",
            rows=list(h2o_policies.values()) + h2o_residuals + h2o_point_inputs,
            excluded_devices=excluded_devices,
            source_table="h2o_policy_residual_or_point_input",
        )
    )
    excluded_rows.extend(
        _excluded_scope_placeholders(
            devices=excluded_devices,
            existing_rows=excluded_rows,
        )
    )

    target_component_count = len(target_devices) * 2
    a_count = sum(1 for row in device_rows if row["fit_input_grade"] == "A")
    summary = [
        {
            "created_at": _now(),
            "run_status": "pass" if input_chain_ready and a_count == target_component_count else "blocked",
            "fit_input_continuity_gate_status": "pass" if continuity_state["ready"] else "blocked",
            "fit_input_continuity_gate_reason": continuity_state["reason"],
            "formal_run_status_json": continuity_state["formal_run_status_json"],
            "mature_route_continuity_gate_json": continuity_state["mature_route_continuity_gate_json"],
            "algorithm_profile_lineage_gate_status": "pass" if lineage_state["ready"] else "blocked",
            "algorithm_profile_lineage_gate_reason": lineage_state["reason"],
            "algorithm_profile_lineage_json": lineage_state["source"],
            "algorithm_profile_id": lineage_contract.get("profile_id", ""),
            "algorithm_profile_sha256": lineage_contract.get("profile_sha256", ""),
            "algorithm_mode": lineage_contract.get("algorithm_mode", ""),
            "co2_fit_input_variable": lineage_contract.get("co2_fit_input", ""),
            "h2o_fit_input_variable": lineage_contract.get("h2o_fit_input", ""),
            "target_device_ids": ";".join(sorted(target_devices)),
            "excluded_device_ids": ";".join(sorted(excluded_devices)),
            "target_component_count": target_component_count,
            "a_grade_component_count": a_count,
            "rejected_component_count": target_component_count - a_count,
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
            "physical_meaning": (
                "This audit freezes the fit-input contract before any coefficient write: only A-grade "
                "open-flow ratio/temperature evidence may feed CO2/H2O candidates; excluded devices remain "
                "diagnostic evidence only."
            ),
        }
    ]

    return {
        "summary": summary,
        "device_quality": device_rows,
        "point_quality": point_rows,
        "excluded_evidence": excluded_rows,
    }


def write_fit_input_quality_report(
    *,
    co2_policy_csv: str | Path,
    co2_residuals_csv: str | Path,
    h2o_policy_csv: str | Path,
    h2o_residuals_csv: str | Path,
    output_dir: str | Path,
    h2o_point_inputs_csv: str | Path | None = None,
    formal_run_status_json: str | Path | None = None,
    mature_route_continuity_gate_json: str | Path | None = None,
    algorithm_profile_lineage_json: str | Path | None = None,
    cfg: FitInputQualityConfig = FitInputQualityConfig(),
) -> Dict[str, Path]:
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    tables = build_fit_input_quality_tables(
        co2_policy_csv=co2_policy_csv,
        co2_residuals_csv=co2_residuals_csv,
        h2o_policy_csv=h2o_policy_csv,
        h2o_residuals_csv=h2o_residuals_csv,
        h2o_point_inputs_csv=h2o_point_inputs_csv,
        formal_run_status_json=formal_run_status_json,
        mature_route_continuity_gate_json=mature_route_continuity_gate_json,
        algorithm_profile_lineage_json=algorithm_profile_lineage_json,
        cfg=cfg,
    )
    paths = {
        "summary": output / "v1_5_fit_input_quality_summary.csv",
        "device_quality": output / "v1_5_fit_input_quality_devices.csv",
        "point_quality": output / "v1_5_fit_input_quality_points.csv",
        "excluded_evidence": output / "v1_5_fit_input_quality_excluded.csv",
        "metadata": output / "v1_5_fit_input_quality_meta.json",
        "markdown": output / "v1_5_fit_input_quality_audit.md",
    }
    for key in ("summary", "device_quality", "point_quality", "excluded_evidence"):
        _write_csv(paths[key], tables[key])
    _write_json(
        paths["metadata"],
        {
            "tool": "v1_5_fit_input_quality",
            "created_at": _now(),
            "inputs": {
                "co2_policy_csv": str(Path(co2_policy_csv).resolve()),
                "co2_residuals_csv": str(Path(co2_residuals_csv).resolve()),
                "h2o_policy_csv": str(Path(h2o_policy_csv).resolve()),
                "h2o_residuals_csv": str(Path(h2o_residuals_csv).resolve()),
                "h2o_point_inputs_csv": str(Path(h2o_point_inputs_csv).resolve()) if h2o_point_inputs_csv else "",
                "formal_run_status_json": (
                    str(Path(formal_run_status_json).resolve()) if formal_run_status_json else ""
                ),
                "mature_route_continuity_gate_json": (
                    str(Path(mature_route_continuity_gate_json).resolve())
                    if mature_route_continuity_gate_json
                    else ""
                ),
                "algorithm_profile_lineage_json": (
                    str(Path(algorithm_profile_lineage_json).resolve())
                    if algorithm_profile_lineage_json
                    else ""
                ),
            },
            "config": {
                "target_device_ids": list(cfg.target_device_ids),
                "excluded_device_ids": list(cfg.excluded_device_ids),
                "co2_min_fit_samples": cfg.co2_min_fit_samples,
                "h2o_min_complete_points": cfg.h2o_min_complete_points,
                "h2o_min_wet_points": cfg.h2o_min_wet_points,
            },
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
        },
    )
    _write_markdown(paths["markdown"], tables)
    return paths


def _write_markdown(path: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> Path:
    summary = (tables.get("summary") or [{}])[0]
    lines = [
        "# V1.5 Fit Input Quality Audit",
        "",
        "- Boundary: offline/no-write; no COM, no route control, no SENCO write.",
        f"- Run status: `{summary.get('run_status', '')}`.",
        f"- Mature route continuity gate: `{summary.get('fit_input_continuity_gate_status', '')}`.",
        f"- Algorithm profile lineage gate: `{summary.get('algorithm_profile_lineage_gate_status', '')}`.",
        f"- Algorithm profile: `{summary.get('algorithm_profile_id', '')}`.",
        f"- CO2/H2O fit variables: `{summary.get('co2_fit_input_variable', '')}` / `{summary.get('h2o_fit_input_variable', '')}`.",
        f"- Continuity reason: `{summary.get('fit_input_continuity_gate_reason', '')}`.",
        f"- Target devices: `{summary.get('target_device_ids', '')}`.",
        f"- Excluded devices: `{summary.get('excluded_device_ids', '')}`.",
        "",
        "| Component | Device | Fit Grade | Status | Fit Points | Reject Reasons | Warnings |",
        "| --- | --- | --- | --- | ---: | --- | --- |",
    ]
    for row in tables.get("device_quality", []):
        lines.append(
            "| {component} | {device} | {grade} | {status} | {points} | {rejects} | {warnings} |".format(
                component=row.get("component", ""),
                device=row.get("device_id", ""),
                grade=row.get("fit_input_grade", ""),
                status=row.get("fit_input_status", ""),
                points=row.get("fit_point_count", ""),
                rejects=row.get("reject_reasons", ""),
                warnings=row.get("warning_reasons", ""),
            )
        )
    lines.extend(
        [
            "",
            "## Physical Meaning",
            "",
            "- A fit-input grade only answers whether the recorded physical evidence is clean enough to feed the model.",
            "- It does not mean the candidate model is already acceptable; residual review, write review, readback, and post-write verification remain separate.",
            "- Devices excluded from the current scope are preserved as diagnostic evidence and cannot influence coefficient fitting.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path
