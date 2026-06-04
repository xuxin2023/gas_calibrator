"""Offline V1.5 component-coefficient write-review package.

This module reviews already-exported candidate coefficient artifacts. It never
opens COM ports, controls gas/water routes, controls PACE/valves, or writes
SENCO coefficients. Its output is a pre-write review surface, not a writer.
"""

from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from ..senco_format import format_senco_values
from .formal_open_flow_artifacts import load_plan_snapshot
from .reporting import ValidationMetadata, write_validation_report


FORMULA_CONTRACT_UNCONFIRMED = "unconfirmed"
FORMULA_CONTRACT_MANUAL_SENCO13_RT_PRESSURE_SEPARATE = "manual_senco13_rt_pressure_separate_v1_5"
SENCO5_POLICY_BLOCKED = "blocked"
SENCO5_POLICY_PRESERVE_EXISTING = "preserve_existing_senco5_senco6_linear_correction"
SENCO5_POLICY_PRESERVE_EXISTING_LEGACY = "preserve_existing_senco5_density_temperature"
SENCO5_POLICY_INTEGRATED_OUTPUT_LAYER_REVIEWED = "integrated_senco5_senco6_output_layer_reviewed"
SENCO5_LINEAR_CORRECTION_CHECK = "co2_senco5_senco6_linear_correction_contract"
PRESSURE_TERMS = ("P", "RP", "RTP")
PRESSURE_TERM_ZERO_ATOL = 1e-12


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _read_csv(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "None", "null"):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _senco5_policy_preserves_existing(value: Any) -> bool:
    policy = str(value or "").strip()
    return policy in {SENCO5_POLICY_PRESERVE_EXISTING, SENCO5_POLICY_PRESERVE_EXISTING_LEGACY}


def _senco5_policy_integrated_reviewed(value: Any) -> bool:
    return str(value or "").strip() == SENCO5_POLICY_INTEGRATED_OUTPUT_LAYER_REVIEWED


def _compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


def _standard_gas_rows(plan: Mapping[str, Any], component: str) -> List[Dict[str, Any]]:
    gases = plan.get("standard_gases")
    if not isinstance(gases, Sequence) or isinstance(gases, (str, bytes)):
        return []
    out: List[Dict[str, Any]] = []
    for gas in gases:
        if not isinstance(gas, Mapping):
            continue
        if str(gas.get("component") or "").strip().lower() != component:
            continue
        out.append(
            {
                "component": component,
                "cylinder_id": gas.get("cylinder_id", ""),
                "certificate_id": gas.get("certificate_id", ""),
                "certificate_value": gas.get("certificate_value", ""),
                "certificate_uncertainty": gas.get("certificate_uncertainty", ""),
                "certificate_uncertainty_unit": gas.get("certificate_uncertainty_unit", ""),
                "valid_until": gas.get("valid_until", ""),
                "traceability_level": gas.get("traceability_level", ""),
                "formal_release_status": gas.get("formal_release_status", ""),
            }
        )
    return out


def _check_row(check: str, status: str, meaning: str, evidence: Any = "") -> Dict[str, Any]:
    return {
        "check": check,
        "status": status,
        "meaning": meaning,
        "evidence": evidence,
    }


def _component_senco_channels(component: str) -> tuple[int, int]:
    text = str(component or "").strip().lower()
    if text == "h2o":
        return 2, 4
    return 1, 3


def _old_snapshot_device(snapshot: Optional[Mapping[str, Any]], device_id: str) -> Mapping[str, Any]:
    if not isinstance(snapshot, Mapping):
        return {}
    normalized = str(device_id or "").strip()
    direct = snapshot.get(normalized)
    if isinstance(direct, Mapping):
        return direct
    devices = snapshot.get("devices")
    if isinstance(devices, Sequence) and not isinstance(devices, (str, bytes)):
        for item in devices:
            if not isinstance(item, Mapping):
                continue
            item_device = str(
                item.get("analyzer_device_id")
                or item.get("device_id")
                or item.get("DeviceId")
                or item.get("id")
                or ""
            ).strip()
            if item_device == normalized:
                return item
    return {}


def _snapshot_channel_values(device_snapshot: Mapping[str, Any], channel: int) -> Any:
    keys = (
        f"GETCO{channel}_before_live",
        f"GETCO{channel}_before_review",
        f"GETCO{channel}_before",
        f"SENCO{channel}_before_live",
        f"SENCO{channel}_before_review",
        f"SENCO{channel}_before",
        f"target_senco{channel}_payload_values",
        f"GETCO{channel}",
        f"SENCO{channel}",
        str(channel),
    )
    for key in keys:
        value = device_snapshot.get(key)
        if value not in (None, ""):
            return value
    return ""


def _snapshot_numeric_values(value: Any) -> List[float]:
    if value in (None, ""):
        return []
    candidate = value
    if isinstance(value, str):
        try:
            candidate = json.loads(value)
        except Exception:
            candidate = value
    if isinstance(candidate, Mapping):
        out: List[float] = []
        for index in range(6):
            numeric = _safe_float(candidate.get(f"C{index}"))
            if numeric is not None:
                out.append(float(numeric))
        return out
    if isinstance(candidate, Sequence) and not isinstance(candidate, (str, bytes)):
        out = []
        for item in candidate:
            numeric = _safe_float(item)
            if numeric is not None:
                out.append(float(numeric))
        return out
    return []


def _snapshot_values_complete(value: Any, *, min_count: int = 4) -> bool:
    if value in (None, ""):
        return False
    candidate = value
    if isinstance(value, str):
        try:
            candidate = json.loads(value)
        except Exception:
            candidate = value
    if isinstance(candidate, Mapping):
        keys = [key for key in candidate if str(key).upper().startswith("C")]
        return len(keys) >= int(min_count)
    if isinstance(candidate, Sequence) and not isinstance(candidate, (str, bytes)):
        return len(candidate) >= int(min_count)
    return False


def _coefficients_by_device(coefficients: Sequence[Mapping[str, Any]], component: str) -> Dict[str, Dict[str, float]]:
    out: Dict[str, Dict[str, float]] = {}
    for row in coefficients:
        if str(row.get("component") or "").strip().lower() != component:
            continue
        device_id = str(row.get("analyzer_device_id") or "").strip()
        term = str(row.get("term") or "").strip()
        value = _safe_float(row.get("coefficient"))
        if not device_id or not term or value is None:
            continue
        out.setdefault(device_id, {})[term] = float(value)
    return out


def _candidate_mapping_rows(
    *,
    policies: Sequence[Mapping[str, Any]],
    coefficients: Sequence[Mapping[str, Any]],
    component: str,
    old_coefficients_snapshot: Optional[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    primary_senco, secondary_senco = _component_senco_channels(component)
    coeff_by_device = _coefficients_by_device(coefficients, component)
    rows: List[Dict[str, Any]] = []
    for policy in policies:
        device_id = str(policy.get("analyzer_device_id") or "").strip()
        prefix = str(policy.get("analyzer_prefix") or "").strip()
        terms = coeff_by_device.get(device_id, {})
        pressure_terms = {
            term: float(value)
            for term in PRESSURE_TERMS
            if (value := _safe_float(terms.get(term))) is not None
        }
        pressure_terms_nonzero = {
            term: value for term, value in pressure_terms.items() if abs(float(value)) > PRESSURE_TERM_ZERO_ATOL
        }
        primary_values = [terms.get(term) for term in ("intercept", "R", "R2", "R3")]
        primary_complete = all(value is not None for value in primary_values)
        primary_payload = [float(value or 0.0) for value in primary_values] + [0.0, 0.0]
        secondary_values = [terms.get(term) for term in ("T", "T2", "RT")]
        secondary_complete = all(value is not None for value in secondary_values)
        device_snapshot = _old_snapshot_device(old_coefficients_snapshot, device_id)
        old_primary = _snapshot_channel_values(device_snapshot, primary_senco)
        old_secondary = _snapshot_channel_values(device_snapshot, secondary_senco)
        old_primary_complete = _snapshot_values_complete(old_primary, min_count=4)
        old_secondary_complete = _snapshot_values_complete(old_secondary, min_count=4)
        old_secondary_values = _snapshot_numeric_values(old_secondary)
        secondary_payload = [
            float(terms.get("T") or 0.0),
            float(terms.get("T2") or 0.0),
            float(terms.get("RT") or 0.0),
            0.0,
            0.0,
            0.0,
        ]
        secondary_payload_preview = secondary_payload[: max(4, len(old_secondary_values) or 6)]
        old_snapshot_status = (
            "primary_and_secondary_bound"
            if old_primary_complete and old_secondary_complete
            else "partial_or_missing"
        )
        if primary_complete and secondary_complete and pressure_terms_nonzero:
            mapping_status = "blocked_pressure_terms_present_current_atmosphere_contract"
            candidate_terms = "intercept;R;R2;R3;T;T2;RT;" + ";".join(pressure_terms_nonzero)
            secondary_action = "blocked_pressure_terms_present_current_atmosphere_contract"
            secondary_command_preview = ""
            reason = (
                "candidate coefficient file contains nonzero P/RP/RTP terms; V1.5 formal CO2/H2O main calibration "
                "uses current-atmosphere open-flow component samples, so pressure terms must be handled only by "
                "the independent SENCO9 pressure-channel workflow"
            )
        elif primary_complete and secondary_complete:
            mapping_status = "review_only_primary_secondary_preview_ready"
            candidate_terms = "intercept;R;R2;R3;T;T2;RT"
            secondary_action = "paired_write_preview_temperature_terms_pressure_slots_zero"
            secondary_command_preview = f"SENCO{secondary_senco},YGAS,FFF," + ",".join(
                format_senco_values(secondary_payload_preview)
            )
            reason = (
                "candidate model supplies primary ratio terms plus secondary T/T2/RT terms; "
                "P/RTP are not fitted at current atmosphere and are kept at zero in the target payload; "
                "pressure is handled by the independent SENCO9 pressure-channel workflow"
            )
        elif primary_complete:
            mapping_status = "review_only_primary_preview_ready"
            candidate_terms = "intercept;R;R2;R3"
            secondary_action = "preserve_existing_requires_old_snapshot_and_manual_mapping_review"
            secondary_command_preview = ""
            reason = (
                "candidate model supplies primary four ratio-polynomial terms only; "
                "secondary SENCO must not be cleared or overwritten without old snapshot review"
            )
        else:
            mapping_status = "blocked_candidate_terms_incomplete"
            candidate_terms = "intercept;R;R2;R3"
            secondary_action = "blocked_candidate_terms_incomplete"
            secondary_command_preview = ""
            reason = "candidate terms are incomplete"
        rows.append(
            {
                "component": component,
                "analyzer_prefix": prefix,
                "analyzer_device_id": device_id,
                "primary_senco": f"SENCO{primary_senco}",
                "secondary_senco": f"SENCO{secondary_senco}",
                "candidate_terms": candidate_terms,
                "candidate_terms_complete": primary_complete,
                "secondary_candidate_terms_complete": secondary_complete,
                "primary_candidate_values": ",".join(format_senco_values(primary_payload)) if primary_complete else "",
                "primary_command_preview": (
                    f"SENCO{primary_senco},YGAS,FFF," + ",".join(format_senco_values(primary_payload))
                    if primary_complete
                    else ""
                ),
                "secondary_candidate_values": (
                    ",".join(format_senco_values(secondary_payload)) if secondary_complete else ""
                ),
                "secondary_action": secondary_action,
                "secondary_command_preview": secondary_command_preview,
                "secondary_pressure_terms_present": ";".join(pressure_terms),
                "secondary_pressure_terms_nonzero": ";".join(
                    f"{term}={value:.12g}" for term, value in pressure_terms_nonzero.items()
                ),
                "secondary_pressure_target_slots_zero": True,
                "old_primary_snapshot": _compact_json(old_primary) if old_primary not in (None, "") else "",
                "old_secondary_snapshot": _compact_json(old_secondary) if old_secondary not in (None, "") else "",
                "old_snapshot_status": old_snapshot_status,
                "old_primary_snapshot_complete": old_primary_complete,
                "old_secondary_snapshot_complete": old_secondary_complete,
                "mapping_status": mapping_status,
                "write_allowed": False,
                "reason": reason,
            }
        )
    return rows


def _candidate_algorithm_alignment_rows(
    component: str,
    *,
    formula_contract: str = FORMULA_CONTRACT_UNCONFIRMED,
    senco5_policy: str = SENCO5_POLICY_BLOCKED,
) -> List[Dict[str, Any]]:
    primary_senco, secondary_senco = _component_senco_channels(component)
    component_upper = str(component or "").strip().upper() or "CO2"
    formula_confirmed = (
        str(formula_contract or "").strip() == FORMULA_CONTRACT_MANUAL_SENCO13_RT_PRESSURE_SEPARATE
    )
    senco5_preserved = _senco5_policy_preserves_existing(senco5_policy)
    senco5_integrated_reviewed = _senco5_policy_integrated_reviewed(senco5_policy)
    rows = [
        {
            "topic": "firmware_formula_contract",
            "current_v1_5": (
                "manual_contract: raw CO2 uses SENCO1 primary R polynomial plus SENCO3 T/T2/RT terms; "
                "firmware displayed CO2 then applies H2O dry-basis correction; SENCO9 pressure is validated separately "
                "and P/RP/RTP are excluded from the V1.5 current-atmosphere open-flow fit"
                if formula_confirmed
                else "direct_ratio_polynomial_candidate_without_zero_baseline_contract"
            ),
            "legacy_v1_v2": "ratio_poly_rt_p; older evidence did not prove the newer normalized-absorbance firmware path",
            "alignment_status": (
                "confirmed_manual_senco13_rt_pressure_separate"
                if formula_confirmed
                else "not_confirmed_for_firmware_final_output_model"
            ),
            "calibration_meaning": (
                "Manual evidence states CO2 concentration fit coefficients are written to SENCO1 and SENCO3; "
                "R is CO2 ratio, T is Kelvin temperature, and P is pressure. Recorded MODE2 evidence shows the "
                "displayed ppm also follows the analyzer H2O dry-basis correction. V1.5 therefore treats pressure "
                "and abnormal H2O channel state as independent inputs, not as CO2 fit terms."
                if formula_confirmed
                else (
                    f"{component_upper} SENCO write must be blocked until the candidate package proves whether the "
                    "instrument firmware evaluates raw/filtered ratio R directly or the newer zero-baseline normalized "
                    "absorbance AbsFinal path."
                )
            ),
        },
        {
            "topic": "coefficient_family",
            "current_v1_5": (
                "point_mean_manual_senco13_ratio_temperature_candidate"
                if formula_confirmed
                else "point_mean_direct_ratio_polynomial_candidate"
            ),
            "legacy_v1_v2": "ratio_poly_rt_p",
            "alignment_status": (
                "manual_contract_confirmed_for_concentration_candidate"
                if formula_confirmed
                else "requires_firmware_formula_confirmation"
            ),
            "calibration_meaning": (
                "Formal fitting uses QC-approved open-flow point means and the manual SENCO1/SENCO3 concentration "
                "mapping. This supports the raw CO2 chain; displayed CO2 must still be replayed through the H2O "
                "dry-basis layer before comparing with firmware output."
                if formula_confirmed
                else (
                    f"{component_upper} candidate still uses analyzer optical ratio R as the fitted input; "
                    "formal fitting uses QC-approved open-flow point means instead of raw-frame weighting, but this "
                    "does not by itself prove the same input variable is used inside the device after SENCO write."
                )
            ),
        },
        {
            "topic": "senco_channel_mapping",
            "current_v1_5": f"SENCO{primary_senco} primary, SENCO{secondary_senco} secondary preserved or paired when T/T2/RT is identifiable",
            "legacy_v1_v2": f"SENCO{primary_senco} primary, SENCO{secondary_senco} secondary",
            "alignment_status": "mapping_consistent",
            "calibration_meaning": "Device command channels match the old V1/V2 download convention.",
        },
        {
            "topic": "terms_written_by_candidate",
            "current_v1_5": "intercept;R;R2;R3 -> primary payload; T/T2/RT -> secondary payload when multi-temperature evidence identifies them",
            "legacy_v1_v2": "a0..a3 primary, a4..a8 secondary when full model is available",
            "alignment_status": "primary_or_identifiable_secondary_only",
            "calibration_meaning": (
                "Current open-flow single-pressure data can identify ratio and temperature terms when enough "
                "temperature span exists; pressure terms remain frozen unless a separate valid pressure span exists."
            ),
        },
        {
            "topic": "pressure_temperature_terms",
            "current_v1_5": "P/T terms frozen by default; SENCO9 pressure handled separately",
            "legacy_v1_v2": "ratio_poly_rt_p can include R/T/P features if data span supports them",
            "alignment_status": "intentional_v1_5_scope_reduction",
            "calibration_meaning": (
                "CO2/H2O main calibration must not use contaminated sealed pressure points; pressure input P is "
                "validated or calibrated independently before component candidate review."
            ),
        },
        {
            "topic": "write_policy",
            "current_v1_5": "no-write review; old GETCO snapshot, mapping review, approval, post-write verification required",
            "legacy_v1_v2": "download plan can generate SENCO commands after fit",
            "alignment_status": "safer_than_legacy_direct_write",
            "calibration_meaning": "Candidate math output is not treated as a device write instruction until evidence is complete.",
        },
    ]
    if component_upper == "CO2":
        rows.insert(
            3,
            {
                "topic": "co2_senco5_senco6_linear_correction_scope",
                "current_v1_5": (
                    "SENCO5/SENCO6 are preserved unchanged; final CO2/H2O concentration linear-trim layers remain outside this SENCO1/SENCO3 write"
                    if senco5_preserved
                    else (
                        "SENCO5/SENCO6 are included in the same final-output candidate package"
                        if senco5_integrated_reviewed
                        else "SENCO5/SENCO6 are final CO2/H2O concentration affine layers but are not identified by the lower-layer direct-ratio candidate"
                    )
                ),
                "legacy_v1_v2": "older download plans generally wrote SENCO1/SENCO3 for CO2 ratio polynomial output",
                "alignment_status": (
                    "preserve_existing_not_part_of_concentration_candidate"
                    if senco5_preserved
                    else (
                        "integrated_final_output_layer_reviewed"
                        if senco5_integrated_reviewed
                        else "in_scope_requires_integrated_output_layer_review"
                    )
                ),
                "calibration_meaning": (
                    "Manual review treats SENCO5/SENCO6 as separate final concentration affine trims "
                    "(corrected = measured*C1 + C0). Under this "
                    "V1.5 contract they are not fitted from the SENCO1/SENCO3 "
                    "optical/temperature residuals and are not written."
                    if senco5_preserved
                    else (
                        "SENCO5/SENCO6 have been reviewed as the final displayed-concentration output layer in the "
                        "same candidate package."
                        if senco5_integrated_reviewed
                        else "SENCO5/SENCO6 need an integrated final-concentration output-layer contract with old GETCO backup and "
                        "independent verification before any write."
                    )
                ),
            },
        )
    return rows


def build_candidate_write_review_tables(
    *,
    candidate_dir: str | Path,
    plan: Mapping[str, Any],
    component: str = "co2",
    min_fit_points: int = 5,
    old_coefficients_snapshot: Optional[Mapping[str, Any]] = None,
    formula_contract: str = FORMULA_CONTRACT_UNCONFIRMED,
    senco5_policy: str = SENCO5_POLICY_BLOCKED,
) -> tuple[Dict[str, List[Dict[str, Any]]], Dict[str, Any]]:
    """Build offline component-coefficient write-review tables."""

    root = Path(candidate_dir).resolve()
    run_summary = _read_csv(root / "candidate_run_summary.csv")
    policies = _read_csv(root / "candidate_policy_summary.csv")
    coefficients = _read_csv(root / "candidate_coefficients.csv")
    verification = _read_csv(root / "candidate_verification_summary.csv")

    summary_row = run_summary[0] if run_summary else {}
    run_status = str(summary_row.get("candidate_run_status") or "").strip()
    no_write_boundary_ok = (
        str(summary_row.get("auto_write_allowed") or "").strip() == "False"
        and str(summary_row.get("opens_com_ports") or "").strip() == "False"
        and str(summary_row.get("controls_water_or_gas_routes") or "").strip() == "False"
        and str(summary_row.get("writes_coefficients") or "").strip() == "False"
    )
    def _row_ready_for_write_review(row: Mapping[str, Any]) -> bool:
        status = str(row.get("candidate_status") or "").strip()
        if status == "verification_passed":
            status_ok = _truthy(row.get("allowed_for_review"))
        elif status == "fit_ready_requires_verification":
            status_ok = _truthy(row.get("allowed_to_fit"))
        else:
            status_ok = False
        return status_ok and int(_safe_float(row.get("fit_point_count")) or 0) >= int(min_fit_points)

    ready_rows = [row for row in policies if _row_ready_for_write_review(row)]
    blocked_rows = [row for row in policies if row not in ready_rows]
    device_count = len(policies)
    coefficient_device_ids = {
        str(row.get("analyzer_device_id") or "").strip()
        for row in coefficients
        if str(row.get("component") or "").strip().lower() == component
    }
    ready_device_ids = {
        str(row.get("analyzer_device_id") or "").strip()
        for row in ready_rows
    }
    coefficients_present_for_ready = ready_device_ids.issubset(coefficient_device_ids)
    mapping_rows = _candidate_mapping_rows(
        policies=policies,
        coefficients=coefficients,
        component=component,
        old_coefficients_snapshot=old_coefficients_snapshot,
    )
    mapping_by_device = {
        str(row.get("analyzer_device_id") or "").strip(): row for row in mapping_rows
    }
    old_snapshot_ready_device_ids = {
        str(row.get("analyzer_device_id") or "").strip()
        for row in mapping_rows
        if str(row.get("old_snapshot_status") or "") == "primary_and_secondary_bound"
    }
    pressure_terms_frozen = bool(ready_device_ids) and all(
        not str(row.get("secondary_pressure_terms_nonzero") or "").strip()
        for row in mapping_rows
        if str(row.get("analyzer_device_id") or "").strip() in ready_device_ids
    )
    mapping_preview_ready = bool(mapping_rows) and all(
        str(row.get("mapping_status") or "")
        in {"review_only_primary_preview_ready", "review_only_primary_secondary_preview_ready"}
        for row in mapping_rows
        if str(row.get("analyzer_device_id") or "").strip() in ready_device_ids
    )
    old_snapshot_bound = bool(ready_device_ids) and ready_device_ids.issubset(old_snapshot_ready_device_ids)
    run_reviewable = run_status in {"verification_passed", "fit_ready_requires_verification"}
    candidate_run_check_status = (
        "pass"
        if run_status == "verification_passed"
        else "review_only_requires_post_write_verification"
        if run_status == "fit_ready_requires_verification"
        else "fail"
    )
    candidate_run_check_meaning = (
        "Candidate run passed independent verification before write review."
        if run_status == "verification_passed"
        else "Candidate fit is reviewable, but independent verification is intentionally deferred to the post-write small verification run."
        if run_status == "fit_ready_requires_verification"
        else "Candidate run must be fit-ready or independently verification-passed before write review."
    )
    formula_contract_value = str(formula_contract or "").strip()
    formula_contract_confirmed = formula_contract_value == FORMULA_CONTRACT_MANUAL_SENCO13_RT_PRESSURE_SEPARATE
    senco5_policy_value = str(senco5_policy or "").strip()
    senco5_preserve_existing = _senco5_policy_preserves_existing(senco5_policy_value)
    senco5_integrated_reviewed = _senco5_policy_integrated_reviewed(senco5_policy_value)
    firmware_formula_status = "pass" if formula_contract_confirmed else "block_write"
    firmware_formula_meaning = (
        "Manual formula contract fixed for V1.5 CO2: SENCO1 holds primary ratio terms, SENCO3 holds identifiable "
        "temperature terms, the displayed firmware ppm applies H2O dry-basis correction, and pressure terms are "
        "excluded from the current-atmosphere component fit because SENCO9 is validated/calibrated independently."
        if formula_contract_confirmed
        else (
            "Direct ratio-polynomial candidates are review evidence only until the firmware-side formula contract "
            "is confirmed against the displayed firmware ppm layer, including analyzer H2O dry-basis correction."
        )
    )
    firmware_formula_evidence = (
        "manual_doc: CO2 concentration fit coefficients write to SENCO1/SENCO3; R=CO2 ratio,T=Kelvin,P=pressure; "
        "V1.5 pressure handled by SENCO9 workflow; recorded MODE2 replay confirms H2O dry-basis final ppm layer"
        if formula_contract_confirmed
        else "manual_senco13_plus_h2o_final_output_contract_required"
    )
    if str(component or "").strip().lower() == "co2":
        senco5_status = "pass" if (senco5_preserve_existing or senco5_integrated_reviewed) else "block_write"
        senco5_meaning = (
            "SENCO5/SENCO6 are preserved as separate final-concentration linear-trim layers. This SENCO1/SENCO3 "
            "candidate does not authorize a SENCO5/SENCO6 write."
            if senco5_preserve_existing
            else (
                "SENCO5/SENCO6 are part of the reviewed final displayed-concentration output candidate package."
                if senco5_integrated_reviewed
                else "Do not write CO2 SENCO1/SENCO3 candidates until SENCO5/SENCO6 are explicitly preserved or "
                "modeled by an integrated final-concentration output-layer review."
            )
        )
        senco5_evidence = (
            "manual_doc: SENCO5/SENCO6 are separate final-concentration linear trims; V1.5 SENCO1/SENCO3 write preserves them"
            if senco5_preserve_existing
            else (
                "manual_doc_and_candidate_package: SENCO5/SENCO6 final concentration affine layer reviewed with SENCO1/SENCO3"
                if senco5_integrated_reviewed
                else "manual_co2_senco5_final_concentration_linear_trim_scope"
            )
        )
    else:
        senco5_status = "not_applicable"
        senco5_meaning = "SENCO5 is a CO2-family group and is not applicable to this component."
        senco5_evidence = ""

    checks = [
        _check_row(
            "candidate_run_verified",
            candidate_run_check_status,
            candidate_run_check_meaning,
            run_status,
        ),
        _check_row(
            "offline_no_write_boundary",
            "pass" if no_write_boundary_ok else "fail",
            "Review package must prove it did not open COM, control routes, or write coefficients.",
            _compact_json(summary_row),
        ),
        _check_row(
            "all_devices_ready_for_reviewer",
            "pass" if device_count > 0 and not blocked_rows else "fail",
            "Every analyzer selected for fleet write review must be fit-ready or verification-passed and allowed for review.",
            f"ready={len(ready_rows)} blocked={len(blocked_rows)}",
        ),
        _check_row(
            "minimum_fit_points",
            "pass" if ready_rows and all(int(_safe_float(row.get("fit_point_count")) or 0) >= min_fit_points for row in ready_rows) else "fail",
            "Component coefficients should be based on point means, not raw frame count weighting.",
            f"min_fit_points={min_fit_points}",
        ),
        _check_row(
            "candidate_coefficients_present",
            "pass" if coefficients_present_for_ready and bool(coefficients) else "fail",
            "Each ready analyzer must have candidate coefficient rows.",
            f"ready_devices={len(ready_device_ids)} coefficient_devices={len(coefficient_device_ids)}",
        ),
        _check_row(
            "old_coefficients_snapshot_bound",
            "pass" if old_snapshot_bound else "block_write",
            "Actual device write requires old component GETCO/SENCO primary and secondary snapshots for rollback.",
            f"ready_devices={len(ready_device_ids)} bound_devices={len(old_snapshot_ready_device_ids)}",
        ),
        _check_row(
            "component_senco_mapping_reviewed",
            "review_only" if mapping_preview_ready else "fail",
            "Candidate math coefficients must be mapped to component SENCO channels without clearing secondary coefficients.",
            f"mapping_preview_ready={mapping_preview_ready}",
        ),
        _check_row(
            "secondary_pressure_terms_frozen",
            "pass" if pressure_terms_frozen else "fail",
            "Current-atmosphere open-flow CO2/H2O samples cannot identify P/RP/RTP terms; those slots must stay zero and pressure stays in SENCO9.",
            f"ready_devices={len(ready_device_ids)} pressure_terms_frozen={pressure_terms_frozen}",
        ),
        _check_row(
            "firmware_formula_contract_confirmed",
            firmware_formula_status,
            firmware_formula_meaning,
            firmware_formula_evidence,
        ),
        _check_row(
            SENCO5_LINEAR_CORRECTION_CHECK,
            senco5_status,
            senco5_meaning,
            senco5_evidence,
        ),
        _check_row(
            "operator_reviewer_approver_required",
            "block_write",
            "A controlled write requires explicit operator confirmation plus separate reviewer/approver.",
            "not_supplied_by_offline_review",
        ),
        _check_row(
            "post_write_verification_required",
            "block_write",
            "Any later controlled write must be followed by an independent open-flow verification point.",
            "required_after_write",
        ),
    ]

    if (
        run_reviewable
        and no_write_boundary_ok
        and not blocked_rows
        and coefficients_present_for_ready
        and pressure_terms_frozen
    ):
        review_status = "ready_for_human_candidate_review"
    else:
        review_status = "blocked"
    write_status = "blocked_until_getco_backup_senco_mapping_approval_and_post_write_plan"

    candidate_rows: List[Dict[str, Any]] = []
    verification_by_device = {
        str(row.get("analyzer_device_id") or "").strip(): row for row in verification
    }
    for row in policies:
        device_id = str(row.get("analyzer_device_id") or "").strip()
        verification_row = verification_by_device.get(device_id, {})
        candidate_rows.append(
            {
                "component": component,
                "analyzer_prefix": row.get("analyzer_prefix", ""),
                "analyzer_device_id": device_id,
                "review_ready": device_id in ready_device_ids,
                "candidate_status": row.get("candidate_status", ""),
                "fit_sample_count": row.get("fit_sample_count", ""),
                "fit_point_count": row.get("fit_point_count", ""),
                "fit_rmse": row.get("fit_rmse", ""),
                "fit_max_error": row.get("fit_max_error", ""),
                "verification_status": verification_row.get("verification_status", row.get("verification_status", "")),
                "verification_max_error": verification_row.get("verification_max_error", row.get("verification_max_error", "")),
                "verification_error_limit": verification_row.get("verification_error_limit", row.get("verification_error_limit", "")),
                "verification_error_limit_source": verification_row.get(
                    "verification_error_limit_source", row.get("verification_error_limit_source", "")
                ),
                "primary_senco": mapping_by_device.get(device_id, {}).get("primary_senco", ""),
                "secondary_senco": mapping_by_device.get(device_id, {}).get("secondary_senco", ""),
                "old_snapshot_status": mapping_by_device.get(device_id, {}).get("old_snapshot_status", ""),
                "mapping_status": mapping_by_device.get(device_id, {}).get("mapping_status", ""),
                "blocked_reasons": row.get("blocked_reasons", ""),
                "warning_reasons": row.get("warning_reasons", ""),
                "write_readiness": "review_only_not_write_ready",
            }
        )

    purge_rows = [
        {
            "route": "co2_open_flow",
            "policy": "minimum_purge_plus_stability_gate",
            "recommended_default_min_purge_s": 360,
            "recommended_low_ppm_or_large_switch_min_purge_s": 360,
            "stability_window_s": 60,
            "adaptive_extension": True,
            "extension_trigger": "ratio_span_or_slope_not_stable",
            "physical_reason": (
                "Open flow must replace dead volume and allow analyzer optical ratio cache/filter to settle."
            ),
            "evidence": "ID100 100 ppm ratio span improved from 0.0125 to 0.0015 after 360 s purge and 60 samples.",
        },
        {
            "route": "h2o_open_flow",
            "policy": "keep_existing_dewpoint_stability_gate",
            "recommended_default_min_purge_s": 360,
            "recommended_low_ppm_or_large_switch_min_purge_s": 360,
            "stability_window_s": "route_specific",
            "adaptive_extension": True,
            "extension_trigger": "dewpoint_or_h2o_dry_ppmv_not_stable",
            "physical_reason": "Water route stability is governed by humidity exchange and dewpoint equilibrium.",
            "evidence": "Minimum purge is 360 s, then water-route dewpoint/H2O dry stability gates still decide sampling readiness.",
        },
    ]

    standard_rows = _standard_gas_rows(plan, component)
    algorithm_rows = _candidate_algorithm_alignment_rows(
        component,
        formula_contract=formula_contract_value,
        senco5_policy=senco5_policy_value,
    )
    summary = [
        {
            "review_status": review_status,
            "write_status": write_status,
            "candidate_dir": str(root),
            "component": component,
            "device_count": device_count,
            "ready_device_count": len(ready_rows),
            "blocked_device_count": len(blocked_rows),
            "coefficient_row_count": len(coefficients),
            "standard_gas_count": len(standard_rows),
            "auto_write_allowed": False,
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
            "physical_scope": "current_atmosphere_open_flow_component_candidate_review",
            "formula_contract": formula_contract_value,
            "senco5_policy": senco5_policy_value,
        }
    ]

    tables = {
        "candidate_write_review_summary": summary,
        "candidate_write_review_checks": checks,
        "candidate_write_review_devices": candidate_rows,
        "candidate_senco_mapping_review": mapping_rows,
        "candidate_algorithm_alignment": algorithm_rows,
        "candidate_write_review_purge_policy": purge_rows,
        "candidate_write_review_standard_gases": standard_rows,
    }
    context = {
        "review_status": review_status,
        "write_status": write_status,
        "ready_device_count": len(ready_rows),
        "formula_contract": formula_contract_value,
        "senco5_policy": senco5_policy_value,
    }
    return tables, context


def _write_markdown_report(destination: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> Path:
    summary = (tables.get("candidate_write_review_summary") or [{}])[0]
    devices = list(tables.get("candidate_write_review_devices") or [])
    mappings = list(tables.get("candidate_senco_mapping_review") or [])
    algorithms = list(tables.get("candidate_algorithm_alignment") or [])
    checks = list(tables.get("candidate_write_review_checks") or [])
    purge_rows = list(tables.get("candidate_write_review_purge_policy") or [])
    path = destination / "candidate_write_review_runbook.md"
    lines = [
        "# V1.5 Component Candidate Write Review",
        "",
        f"- Review status: {summary.get('review_status', '')}",
        f"- Write status: {summary.get('write_status', '')}",
        "- Boundary: offline review only; no COM ports, no PACE/valves, no water/gas route control, no SENCO write.",
        "",
        "## Device Summary",
        "",
        "| Analyzer | Device ID | Review Ready | Fit Points | Fit RMSE | Fit Max Error | Verification Max Error | Limit |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in devices:
        lines.append(
            "| {prefix} | {device} | {ready} | {fit_points} | {fit_rmse} | {fit_max} | {verify_max} | {limit} |".format(
                prefix=row.get("analyzer_prefix", ""),
                device=row.get("analyzer_device_id", ""),
                ready=row.get("review_ready", ""),
                fit_points=row.get("fit_point_count", ""),
                fit_rmse=row.get("fit_rmse", ""),
                fit_max=row.get("fit_max_error", ""),
                verify_max=row.get("verification_max_error", ""),
                limit=row.get("verification_error_limit", ""),
            )
        )
    lines.extend(
        [
            "",
            "## SENCO Mapping Review",
            "",
            "| Analyzer | Device ID | Primary | Secondary | Mapping Status | Old Snapshot | Primary Preview | Secondary Preview |",
            "| --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for row in mappings:
        lines.append(
            "| {prefix} | {device} | {primary} | {secondary} | {status} | {old} | `{primary_preview}` | `{secondary_preview}` |".format(
                prefix=row.get("analyzer_prefix", ""),
                device=row.get("analyzer_device_id", ""),
                primary=row.get("primary_senco", ""),
                secondary=row.get("secondary_senco", ""),
                status=row.get("mapping_status", ""),
                old=row.get("old_snapshot_status", ""),
                primary_preview=row.get("primary_command_preview", ""),
                secondary_preview=row.get("secondary_command_preview", ""),
            )
        )
    lines.extend(
        [
            "",
            "## Algorithm Alignment",
            "",
            "| Topic | V1.5 Current | Legacy V1/V2 | Status |",
            "| --- | --- | --- | --- |",
        ]
    )
    for row in algorithms:
        lines.append(
            "| {topic} | {current} | {legacy} | {status} |".format(
                topic=row.get("topic", ""),
                current=row.get("current_v1_5", ""),
                legacy=row.get("legacy_v1_v2", ""),
                status=row.get("alignment_status", ""),
            )
        )
    lines.extend(["", "## Write Blockers", ""])
    for row in checks:
        if str(row.get("status") or "") != "pass":
            lines.append(f"- {row.get('check')}: {row.get('status')} - {row.get('meaning')}")
    lines.extend(["", "## Purge Policy", ""])
    for row in purge_rows:
        lines.append(f"- {row.get('route')}: {row.get('policy')}; evidence={row.get('evidence')}")
    lines.extend(
        [
            "",
            "## Physical Meaning",
            "",
            "- Component candidate coefficients are reviewed from open-flow point means, while raw frames remain QC evidence.",
            "- Longer purge is justified when low-concentration ratio stability improves after dead-volume replacement and filter/cache settling.",
            "- CO2/H2O writes remain blocked until old coefficients are backed up, SENCO mapping is reviewed, approval is recorded, and post-write verification is planned.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def write_candidate_write_review_report(
    *,
    candidate_dir: str | Path,
    output_dir: str | Path,
    plan: Optional[Mapping[str, Any]] = None,
    plan_path: str | Path | None = None,
    component: str = "co2",
    min_fit_points: int = 5,
    old_coefficients_snapshot: Optional[Mapping[str, Any]] = None,
    formula_contract: str = FORMULA_CONTRACT_UNCONFIRMED,
    senco5_policy: str = SENCO5_POLICY_BLOCKED,
) -> Dict[str, Path]:
    plan_data = dict(plan) if plan is not None else load_plan_snapshot(plan_path)
    tables, context = build_candidate_write_review_tables(
        candidate_dir=candidate_dir,
        plan=plan_data,
        component=component,
        min_fit_points=min_fit_points,
        old_coefficients_snapshot=old_coefficients_snapshot,
        formula_contract=formula_contract,
        senco5_policy=senco5_policy,
    )
    destination = Path(output_dir).resolve()
    metadata = ValidationMetadata(
        tool_name="export_v1_5_candidate_write_review",
        created_at=_now(),
        analyzers=[row.get("analyzer_prefix", "") for row in tables.get("candidate_write_review_devices", [])],
        input_paths=[
            str(Path(candidate_dir).resolve()),
            str(Path(plan_path).resolve()) if plan_path else "",
        ],
        output_dir=str(destination),
        config_summary={
            "component": component,
            "review_status": context.get("review_status", ""),
            "write_status": context.get("write_status", ""),
            "auto_write_allowed": False,
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
            "formula_contract": context.get("formula_contract", ""),
            "senco5_policy": context.get("senco5_policy", ""),
        },
        notes=[
            "Offline V1.5 component candidate write review.",
            "This package previews primary SENCO payloads for mapping review but does not authorize a write.",
            "V1.5 current candidate fitting is ratio-polynomial-family compatible with old V1/V2, but intentionally reduced to open-flow primary terms.",
            "Actual write remains blocked until old coefficients, mapping review, approval, and post-write verification are bound.",
        ],
    )
    outputs = write_validation_report(
        destination,
        prefix="candidate_write_review",
        metadata=metadata,
        tables=tables,
    )
    outputs["markdown"] = _write_markdown_report(destination, tables)
    return outputs
