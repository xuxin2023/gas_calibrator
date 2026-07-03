"""Offline audit for V1.5 CO2 SENCO1/SENCO3 candidate writes.

This module uses only recorded artifacts. It does not open COM ports, control
routes, or write coefficients. The goal is to compare three separate things:

1. the offline candidate model prediction from the recorded ratio/temperature;
2. the actual analyzer output after a controlled write or observation run;
3. the SENCO payload contract that was actually written.
"""

from __future__ import annotations

import csv
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

from ..senco_format import format_senco_values
from .co2_firmware_contract import (
    co2_dry_route_h2o_status,
    co2_h2o_dry_correction_factor,
    co2_raw_to_firmware_final_ppm,
)


CO2_DIRECT_RATIO_TERMS = ("intercept", "R", "R2", "R3", "T", "T2", "RT")


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _read_csv(path: str | Path) -> List[Dict[str, Any]]:
    target = Path(path)
    if not target.exists():
        return []
    with target.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    header: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in header:
                header.append(str(key))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        writer.writerows([dict(row) for row in rows])


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "None", "null"):
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    return numeric if math.isfinite(numeric) else None


def _first_float(row: Mapping[str, Any], *keys: str) -> Optional[float]:
    for key in keys:
        value = _safe_float(row.get(key))
        if value is not None:
            return value
    return None


def _device_id(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _parse_values(value: Any) -> List[float]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        out: List[float] = []
        for item in value:
            numeric = _safe_float(item)
            if numeric is not None:
                out.append(float(numeric))
        return out
    text = str(value or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except Exception:
        parsed = None
    if isinstance(parsed, Sequence) and not isinstance(parsed, (str, bytes)):
        return _parse_values(parsed)
    out: List[float] = []
    for part in text.replace(";", ",").split(","):
        numeric = _safe_float(part.strip())
        if numeric is not None:
            out.append(float(numeric))
    return out


def _load_coefficients(candidate_dir: str | Path) -> Dict[str, Dict[str, float]]:
    rows = _read_csv(Path(candidate_dir) / "candidate_coefficients.csv")
    coeffs: Dict[str, Dict[str, float]] = {}
    for row in rows:
        if str(row.get("component") or "").strip().lower() != "co2":
            continue
        device = _device_id(row.get("analyzer_device_id"))
        term = str(row.get("term") or "").strip()
        value = _safe_float(row.get("coefficient"))
        if not device or not term or value is None:
            continue
        coeffs.setdefault(device, {})[term] = float(value)
    return coeffs


def direct_ratio_model_prediction(coefficients: Mapping[str, float], *, ratio: float, temperature_c: float) -> float:
    """Evaluate the current V1.5 direct-ratio candidate formula.

    Temperature is evaluated in kelvin to match the documented legacy formula.
    Pressure terms are intentionally absent because V1.5 open-flow candidates
    freeze P/RP/RTP.
    """

    temp_k = float(temperature_c) + 273.15
    r = float(ratio)
    return float(
        coefficients.get("intercept", 0.0)
        + coefficients.get("R", 0.0) * r
        + coefficients.get("R2", 0.0) * r**2
        + coefficients.get("R3", 0.0) * r**3
        + coefficients.get("T", 0.0) * temp_k
        + coefficients.get("T2", 0.0) * temp_k**2
        + coefficients.get("RT", 0.0) * r * temp_k
    )


def _write_payload_by_device(write_dir: str | Path | None) -> Dict[str, Mapping[str, Any]]:
    if not write_dir:
        return {}
    rows = _read_csv(Path(write_dir) / "co2_senco13_pair_write_summary.csv")
    return {_device_id(row.get("analyzer_device_id")): row for row in rows if _device_id(row.get("analyzer_device_id"))}


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _snapshot_score(row: Mapping[str, Any], device: str) -> int:
    target = _device_id(row.get("getco_target"))
    if target == device:
        return 30
    if target == "FFF":
        return 20
    if target == "000":
        return 10
    return 0


def _getco_snapshot_by_device(getco_snapshot_csv: str | Path | None) -> Dict[str, Dict[int, List[float]]]:
    """Load the best valid GETCO group response per device.

    The live probe may try FFF, device-specific, and 000 targets. Some firmware
    revisions answer only one of them for final linear groups, so keep the best
    valid response instead of treating the first failed probe as the group state.
    """

    if not getco_snapshot_csv:
        return {}
    rows = _read_csv(getco_snapshot_csv)
    selected: Dict[tuple[str, int], tuple[int, List[float]]] = {}
    for row in rows:
        if not _truthy(row.get("coefficient_valid")):
            continue
        device = _device_id(row.get("analyzer_device_id"))
        group = _safe_float(row.get("getco_group"))
        values = _parse_values(row.get("coefficient_values_json"))
        if not device or group is None or not values:
            continue
        key = (device, int(group))
        score = _snapshot_score(row, device)
        previous = selected.get(key)
        if previous is None or score > previous[0] or (score == previous[0] and len(values) > len(previous[1])):
            selected[key] = (score, values)

    out: Dict[str, Dict[int, List[float]]] = {}
    for (device, group), (_score, values) in selected.items():
        out.setdefault(device, {})[group] = values
    return out


def _getco_values(getco_by_device: Mapping[str, Mapping[int, Sequence[float]]], device: str, group: int) -> List[float]:
    values = getco_by_device.get(device, {}).get(int(group), [])
    return [float(value) for value in values]


def _values_json(values: Sequence[float]) -> str:
    return json.dumps([float(value) for value in values], ensure_ascii=False, separators=(",", ":"))


def _linear_trim_status(values: Sequence[float], *, offset_limit: float = 0.5, gain_limit: float = 0.02) -> str:
    if len(values) < 2:
        return "missing"
    offset = float(values[0])
    gain = float(values[1])
    if abs(offset) <= float(offset_limit) and abs(gain - 1.0) <= float(gain_limit):
        return "neutral"
    return "non_neutral"


def _temperature_group_status(values: Sequence[float]) -> str:
    if len(values) < 2:
        return "missing"
    expected = [0.0, 1.0, 0.0, 0.0]
    padded = list(float(value) for value in values[:4])
    while len(padded) < 4:
        padded.append(0.0)
    if all(abs(got - exp) <= 1.0e-6 for got, exp in zip(padded, expected)):
        return "neutral"
    return "non_neutral"


def _pressure_group_status(values: Sequence[float]) -> str:
    if len(values) < 2:
        return "missing"
    offset = float(values[0])
    gain = float(values[1])
    if abs(offset) <= 0.5 and abs(gain - 1.0) <= 0.01:
        return "near_neutral"
    return "non_neutral_pressure_calibrated_or_review"


def _payload_status(write_row: Mapping[str, Any] | None) -> tuple[str, str, str, str]:
    if not write_row:
        return "not_written_or_not_in_write_summary", "", "", ""
    target1 = _parse_values(write_row.get("target_senco1_values"))
    target3 = _parse_values(write_row.get("target_senco3_values"))
    preview1 = ",".join(format_senco_values(target1)) if target1 else ""
    preview3 = ",".join(format_senco_values(target3)) if target3 else ""
    if len(target1) == 6 and len(target3) == 6:
        return "pass_6_value_payload", str(len(target1)), preview1, preview3
    return f"fail_payload_length_senco1_{len(target1)}_senco3_{len(target3)}", str(len(target1)), preview1, preview3


def _agreement_status(*, observed: float, predicted: Optional[float], certificate: float) -> str:
    if predicted is None:
        return "firmware_prediction_unavailable"
    if observed >= 2999.0 and abs(predicted - certificate) <= 20.0:
        return "device_saturated_while_firmware_model_predicts_in_range"
    if abs(observed - predicted) > 20.0:
        return "device_output_not_reproducing_firmware_model"
    if abs(observed - certificate) > 2.0:
        return "firmware_model_reproduced_but_acceptance_failed"
    return "firmware_model_and_device_agree_near_certificate"


def _raw_model_agreement_status(*, observed: float, predicted: float, certificate: float) -> str:
    if observed >= 2999.0 and abs(predicted - certificate) <= 20.0:
        return "device_saturated_while_raw_model_predicts_in_range"
    if abs(observed - predicted) > 20.0:
        return "device_output_not_reproducing_raw_senco13_model"
    if abs(observed - certificate) > 2.0:
        return "raw_model_reproduced_but_acceptance_failed"
    return "raw_model_and_device_agree_near_certificate"


def _root_cause(row: Mapping[str, Any]) -> str:
    payload_status = str(row.get("payload_status") or "")
    if payload_status.startswith("fail_payload_length"):
        return "writer_payload_or_device_group_width_incompatible"
    agreement = str(row.get("firmware_model_agreement_status") or row.get("direct_model_agreement_status") or "")
    h2o = _safe_float(row.get("h2o_mean_mmol_mol"))
    h2o_status = str(row.get("co2_dry_route_h2o_status") or "")
    if agreement == "device_saturated_while_firmware_model_predicts_in_range":
        return "firmware_output_clamped_or_sensor_state_suspect"
    if agreement in {
        "firmware_model_reproduced_but_acceptance_failed",
        "firmware_model_and_device_agree_near_certificate",
    } and h2o_status in {
        "h2o_high_bias_explains_co2_final_shift",
        "h2o_severe_bias_blocks_co2_acceptance",
    }:
        return "h2o_channel_bias_explains_co2_final_output_shift"
    if h2o is not None and h2o >= 70.0:
        return "h2o_channel_saturated_or_cross_compensation_suspect"
    if agreement == "device_output_not_reproducing_firmware_model":
        if str(row.get("getco6_status") or "") == "non_neutral" and h2o is not None and h2o >= 10.0:
            return "h2o_linear_trim_or_cross_compensation_suspect"
        if str(row.get("getco7_status") or "") == "non_neutral" or str(row.get("getco8_status") or "") == "non_neutral":
            return "temperature_compensation_chain_suspect"
    if agreement == "device_output_not_reproducing_firmware_model":
        return "firmware_formula_contract_mismatch_or_unmodeled_temperature_channel"
    if agreement == "firmware_model_reproduced_but_acceptance_failed":
        return "candidate_data_or_model_bias_at_independent_point"
    return "no_large_mismatch_detected_by_firmware_model_audit"


def _output_chain_gate(agreement: str, root_cause: str, payload_status: str) -> tuple[str, bool, str]:
    if str(payload_status or "").startswith("fail_payload_length"):
        return (
            "blocked_writer_payload_contract",
            True,
            "fix_writer_payload_contract_before_any_write",
        )
    if root_cause == "h2o_channel_bias_explains_co2_final_output_shift":
        return (
            "blocked_h2o_channel_before_co2_acceptance",
            True,
            "validate_or_correct_h2o_channel_before_more_co2_coefficient_writes",
        )
    if root_cause in {
        "firmware_output_clamped_or_sensor_state_suspect",
        "h2o_channel_saturated_or_cross_compensation_suspect",
        "h2o_linear_trim_or_cross_compensation_suspect",
        "temperature_compensation_chain_suspect",
        "firmware_formula_contract_mismatch_or_unmodeled_temperature_channel",
    }:
        return (
            "blocked_output_chain_or_sensor_suspect",
            True,
            "no_more_co2_writes_until_getco_h2o_temperature_pressure_chain_isolated",
        )
    if agreement == "firmware_model_reproduced_but_acceptance_failed":
        return (
            "model_reproduced_candidate_error_review",
            False,
            "review_fit_residuals_then_run_independent_open_flow_recheck",
        )
    return (
        "pass_or_low_risk_output_chain",
        False,
        "eligible_for_independent_open_flow_recheck_after_reviewer_approval",
    )


def build_co2_senco_algorithm_audit_tables(
    *,
    candidate_dir: str | Path,
    verification_summary_csv: str | Path,
    write_dir: str | Path | None = None,
    getco_snapshot_csv: str | Path | None = None,
) -> Dict[str, List[Dict[str, Any]]]:
    coefficients = _load_coefficients(candidate_dir)
    write_rows = _write_payload_by_device(write_dir)
    getco_by_device = _getco_snapshot_by_device(getco_snapshot_csv)
    verification_rows = _read_csv(verification_summary_csv)
    rows: List[Dict[str, Any]] = []
    for source in verification_rows:
        device = _device_id(source.get("device_id") or source.get("analyzer_device_id") or source.get("AnalyzerId"))
        coeffs = coefficients.get(device)
        certificate = _first_float(source, "certificate_co2_ppm", "cert_ppm", "ppm_CO2_Tank")
        observed = _first_float(source, "co2_mean_ppm", "firmware_ppm", "ppm_CO2")
        ratio = _first_float(source, "co2_ratio_f_mean", "R_CO2")
        temp_c = _first_float(source, "chamber_temp_mean_c", "T1_C", "T1")
        if not device or not coeffs or certificate is None or observed is None or ratio is None or temp_c is None:
            continue
        predicted_raw = direct_ratio_model_prediction(coeffs, ratio=ratio, temperature_c=temp_c)
        h2o = _first_float(source, "h2o_mean_mmol_mol", "H2O", "ppm_H2O")
        h2o_factor = co2_h2o_dry_correction_factor(h2o)
        predicted_firmware = co2_raw_to_firmware_final_ppm(predicted_raw, h2o)
        payload_status, payload_len, target1, target3 = _payload_status(write_rows.get(device))
        getco5 = _getco_values(getco_by_device, device, 5)
        getco6 = _getco_values(getco_by_device, device, 6)
        getco7 = _getco_values(getco_by_device, device, 7)
        getco8 = _getco_values(getco_by_device, device, 8)
        getco9 = _getco_values(getco_by_device, device, 9)
        item: Dict[str, Any] = {
            "device_id": device,
            "certificate_co2_ppm": certificate,
            "observed_co2_ppm": observed,
            "co2_ratio_f_mean": ratio,
            "chamber_temp_mean_c": temp_c,
            "pressure_mean_kpa": source.get("pressure_mean_kpa", ""),
            "h2o_mean_mmol_mol": source.get("h2o_mean_mmol_mol", ""),
            "candidate_direct_ratio_prediction_ppm": predicted_raw,
            "candidate_firmware_final_prediction_ppm": predicted_firmware if predicted_firmware is not None else "",
            "h2o_dry_correction_factor": h2o_factor if h2o_factor is not None else "",
            "co2_dry_route_h2o_status": co2_dry_route_h2o_status(h2o),
            "candidate_prediction_error_ppm": predicted_raw - certificate,
            "candidate_firmware_prediction_error_ppm": (
                predicted_firmware - certificate if predicted_firmware is not None else ""
            ),
            "device_error_ppm": observed - certificate,
            "device_minus_candidate_prediction_ppm": observed - predicted_raw,
            "device_minus_candidate_firmware_prediction_ppm": (
                observed - predicted_firmware if predicted_firmware is not None else ""
            ),
            "direct_model_terms": ";".join(CO2_DIRECT_RATIO_TERMS),
            "pressure_terms_frozen": True,
            "payload_status": payload_status,
            "payload_senco1_len": payload_len,
            "target_senco1_scientific_values": target1,
            "target_senco3_scientific_values": target3,
            "getco5_values": _values_json(getco5),
            "getco5_status": _linear_trim_status(getco5),
            "getco6_values": _values_json(getco6),
            "getco6_status": _linear_trim_status(getco6),
            "getco7_values": _values_json(getco7),
            "getco7_status": _temperature_group_status(getco7),
            "getco8_values": _values_json(getco8),
            "getco8_status": _temperature_group_status(getco8),
            "getco9_values": _values_json(getco9),
            "getco9_status": _pressure_group_status(getco9),
            "formula_contract_status": "senco13_raw_ppm_then_h2o_dry_basis_final_ppm",
        }
        item["direct_model_agreement_status"] = _raw_model_agreement_status(
            observed=observed,
            predicted=predicted_raw,
            certificate=certificate,
        )
        item["firmware_model_agreement_status"] = _agreement_status(
            observed=observed,
            predicted=predicted_firmware,
            certificate=certificate,
        )
        item["likely_root_cause"] = _root_cause(item)
        gate, blocker, next_action = _output_chain_gate(
            str(item["firmware_model_agreement_status"]),
            str(item["likely_root_cause"]),
            str(item["payload_status"]),
        )
        item["output_chain_gate"] = gate
        item["output_chain_write_blocker"] = blocker
        item["recommended_next_action"] = next_action
        rows.append(item)

    counts: Dict[str, int] = {}
    for row in rows:
        key = str(row.get("likely_root_cause") or "")
        counts[key] = counts.get(key, 0) + 1
    output_chain_blocked = sum(1 for row in rows if bool(row.get("output_chain_write_blocker")))
    firmware_model_reproduced = sum(
        1
        for row in rows
        if str(row.get("output_chain_gate") or "")
        in {
            "model_reproduced_candidate_error_review",
            "pass_or_low_risk_output_chain",
            "blocked_h2o_channel_before_co2_acceptance",
        }
    )
    summary = [
        {
            "audit_status": (
                "blocked_output_chain_isolation_required"
                if output_chain_blocked
                else "firmware_formula_contract_audited"
            ),
            "candidate_dir": str(Path(candidate_dir).resolve()),
            "verification_summary_csv": str(Path(verification_summary_csv).resolve()),
            "write_dir": str(Path(write_dir).resolve()) if write_dir else "",
            "getco_snapshot_csv": str(Path(getco_snapshot_csv).resolve()) if getco_snapshot_csv else "",
            "device_count": len(rows),
            "output_chain_blocked_count": output_chain_blocked,
            "firmware_model_reproduced_count": firmware_model_reproduced,
            "root_cause_counts_json": json.dumps(counts, ensure_ascii=False, sort_keys=True),
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
            "physical_meaning": (
                "The firmware final CO2 ppm is the raw SENCO1/SENCO3 CO2 result after H2O dry-basis correction. "
                "If this firmware-output model reproduces the device, the remaining acceptance error belongs to the "
                "candidate fit or to the H2O channel; if it does not reproduce the device, the firmware output chain "
                "or sensor state must be isolated before another write."
            ),
        }
    ]
    contract_rows = [
        {
            "topic": "manual_senco_payload",
            "status": "scientific_6_value_payload_required_for_senco1_senco3_pair",
            "meaning": (
                "SENCO1/SENCO3 paired CO2 writes must use the same 6-value scientific payload that was reviewed. "
                "The manual example uses lowercase e notation; sending only the mantissa would change coefficient magnitude."
            ),
        },
        {
            "topic": "manual_formula_contract",
            "status": "senco13_raw_ppm_plus_h2o_dry_basis_correction_matches_displayed_co2",
            "meaning": (
                "SENCO1/SENCO3 identify the raw CO2 ratio-temperature chain. The displayed MODE2 CO2 ppm is then "
                "dry-basis corrected by analyzer H2O output, so offline review must compare against that final layer."
            ),
        },
        {
            "topic": "manual_co2_coefficient_scope",
            "status": "senco1_senco3_main_co2_chain_senco5_not_authorized_by_current_open_flow_fit",
            "meaning": (
                "Manual mapping makes SENCO1 the CO2 ratio/density response, SENCO3 the CO2 ratio-temperature "
                "compensation, and SENCO5 a separate CO2 density-temperature correction layer. Current open-flow "
                "CO2 candidate fitting only authorizes SENCO1/SENCO3 review; SENCO5 stays neutral unless a separate "
                "density-temperature contract is proven."
            ),
        },
        {
            "topic": "h2o_channel_scope",
            "status": "dry_co2_route_h2o_bias_must_not_be_absorbed_into_co2_coefficients",
            "meaning": (
                "High analyzer H2O on dry CO2 gas mathematically inflates displayed CO2 through firmware dry-basis "
                "correction. This is an H2O channel/compensation problem, not permission to bend CO2 SENCO1/SENCO3."
            ),
        },
        {
            "topic": "pressure_scope",
            "status": "pressure_terms_remain_frozen",
            "meaning": "Current open-flow CO2 candidate data do not identify P/RP/RTP; pressure remains a separately verified input.",
        },
    ]
    return {
        "co2_senco_algorithm_audit_summary": summary,
        "co2_senco_algorithm_audit_devices": rows,
        "co2_senco_algorithm_contract": contract_rows,
    }


def write_co2_senco_algorithm_audit(
    *,
    candidate_dir: str | Path,
    verification_summary_csv: str | Path,
    output_dir: str | Path,
    write_dir: str | Path | None = None,
    getco_snapshot_csv: str | Path | None = None,
) -> Dict[str, Path]:
    destination = Path(output_dir).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    tables = build_co2_senco_algorithm_audit_tables(
        candidate_dir=candidate_dir,
        verification_summary_csv=verification_summary_csv,
        write_dir=write_dir,
        getco_snapshot_csv=getco_snapshot_csv,
    )
    outputs: Dict[str, Path] = {}
    for name, rows in tables.items():
        path = destination / f"{name}.csv"
        _write_csv(path, rows)
        outputs[f"{name}_csv"] = path
    meta = {
        "tool_name": "export_v1_5_co2_senco_algorithm_audit",
        "created_at": _now(),
        "inputs": {
            "candidate_dir": str(Path(candidate_dir).resolve()),
            "verification_summary_csv": str(Path(verification_summary_csv).resolve()),
            "write_dir": str(Path(write_dir).resolve()) if write_dir else "",
            "getco_snapshot_csv": str(Path(getco_snapshot_csv).resolve()) if getco_snapshot_csv else "",
        },
        "boundary": {
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
        },
    }
    meta_path = destination / "co2_senco_algorithm_audit_meta.json"
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    outputs["meta_json"] = meta_path
    report_path = destination / "co2_senco_algorithm_audit.md"
    summary = tables["co2_senco_algorithm_audit_summary"][0]
    lines = [
        "# V1.5 CO2 SENCO Algorithm Audit",
        "",
        f"- Status: {summary['audit_status']}",
        "- Boundary: offline only; no COM, no route control, no coefficient write.",
        "",
        "## Conclusion",
        "",
        "- Scientific notation formatting is available and used in reviewed previews.",
        "- Firmware final CO2 prediction now includes the H2O dry-basis correction layer.",
        "- SENCO5 remains outside the current CO2 SENCO1/SENCO3 open-flow fit contract.",
        "- The audit compares both raw SENCO1/SENCO3 prediction and firmware-final prediction against real output.",
        "- GETCO5/6/7/8/9 output-chain state is used when a snapshot is supplied, so sensor/firmware-chain faults are not misfiled as fit residuals.",
        "",
        "## Device Findings",
        "",
        "| Device | Cert ppm | Observed ppm | Raw SENCO13 ppm | Firmware Model ppm | Device-Firmware ppm | Gate | Root Cause | Next Action |",
        "| --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |",
    ]
    for row in tables["co2_senco_algorithm_audit_devices"]:
        lines.append(
            "| {device} | {cert:.3f} | {obs:.3f} | {raw:.3f} | {firmware:.3f} | {delta:.3f} | {gate} | {cause} | {next_action} |".format(
                device=row.get("device_id", ""),
                cert=float(row.get("certificate_co2_ppm") or 0.0),
                obs=float(row.get("observed_co2_ppm") or 0.0),
                raw=float(row.get("candidate_direct_ratio_prediction_ppm") or 0.0),
                firmware=float(row.get("candidate_firmware_final_prediction_ppm") or 0.0),
                delta=float(row.get("device_minus_candidate_firmware_prediction_ppm") or 0.0),
                gate=row.get("output_chain_gate", ""),
                cause=row.get("likely_root_cause", ""),
                next_action=row.get("recommended_next_action", ""),
            )
        )
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    outputs["markdown"] = report_path
    return outputs
