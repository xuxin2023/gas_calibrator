"""No-write CO2 SENCO1/SENCO3 paired review.

This module turns the post-SENCO1-write verification result into a reviewer
artifact. It never opens COM ports, controls gas/water routes, controls PACE,
or writes any analyzer coefficient.
"""

from __future__ import annotations

import csv
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from .reporting import ValidationMetadata, write_validation_report


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _read_csv(path: str | Path) -> List[Dict[str, Any]]:
    target = Path(path)
    if not target.exists():
        return []
    with target.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "None", "null"):
        return None
    try:
        number = float(value)
    except Exception:
        return None
    if not math.isfinite(number):
        return None
    return number


def _relative_error_pct(error: Optional[float], reference: Optional[float]) -> Any:
    if error is None or reference is None or abs(float(reference)) <= 1.0e-12:
        return ""
    return float(float(error) / float(reference) * 100.0)


def _bool_text(value: bool) -> str:
    return "true" if value else "false"


def _compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


def _sha256_file(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _candidate_devices(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    terms_by_device: Dict[str, Dict[str, Any]] = {}
    prefixes: Dict[str, str] = {}
    for row in rows:
        if str(row.get("component") or "").strip().lower() != "co2":
            continue
        device_id = str(row.get("analyzer_device_id") or "").strip()
        term = str(row.get("term") or "").strip()
        if not device_id or not term:
            continue
        prefixes[device_id] = str(row.get("analyzer_prefix") or "").strip()
        terms_by_device.setdefault(device_id, {})[term] = row.get("coefficient")

    devices: List[Dict[str, Any]] = []
    for device_id in sorted(terms_by_device):
        terms = terms_by_device[device_id]
        complete = all(_safe_float(terms.get(term)) is not None for term in ("intercept", "R", "R2", "R3"))
        devices.append(
            {
                "component": "co2",
                "analyzer_prefix": prefixes.get(device_id, ""),
                "analyzer_device_id": device_id,
                "candidate_terms": "intercept;R;R2;R3",
                "candidate_terms_complete": complete,
                "primary_senco": "SENCO1",
                "secondary_senco": "SENCO3",
                "write_allowed": False,
            }
        )
    return devices


def _mapping_by_device(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Mapping[str, Any]]:
    out: Dict[str, Mapping[str, Any]] = {}
    for row in rows:
        device_id = str(row.get("analyzer_device_id") or "").strip()
        if device_id:
            out[device_id] = row
    return out


def _device_diagnostics(
    *,
    verification_rows: Sequence[Mapping[str, Any]],
    mapping_rows: Sequence[Mapping[str, Any]],
    device_output_abs_error_limit_ppm: float,
    primary_model_abs_error_limit_ppm: float,
    device_output_relative_error_limit_pct: float,
    primary_model_relative_error_limit_pct: float,
) -> List[Dict[str, Any]]:
    mapping = _mapping_by_device(mapping_rows)
    diagnostics: List[Dict[str, Any]] = []
    for row in verification_rows:
        device_id = str(row.get("device_id") or row.get("analyzer_device_id") or "").strip()
        analyzer = str(row.get("analyzer") or row.get("analyzer_prefix") or "").strip()
        certificate = _safe_float(row.get("certificate_co2_ppm"))
        device_error = _safe_float(row.get("device_error_ppm"))
        primary_error = _safe_float(row.get("primary_only_error_ppm"))
        device_error_pct = _relative_error_pct(device_error, certificate)
        primary_error_pct = _relative_error_pct(primary_error, certificate)
        output_failed = (
            device_error is None
            or abs(device_error) > float(device_output_abs_error_limit_ppm)
            or (
                isinstance(device_error_pct, float)
                and abs(device_error_pct) > float(device_output_relative_error_limit_pct)
            )
        )
        primary_near = (
            primary_error is not None
            and abs(primary_error) <= float(primary_model_abs_error_limit_ppm)
            and (
                not isinstance(primary_error_pct, float)
                or abs(primary_error_pct) <= float(primary_model_relative_error_limit_pct)
            )
        )
        mapped = mapping.get(device_id, {})
        diagnostics.append(
            {
                "component": "co2",
                "analyzer_prefix": analyzer,
                "analyzer_device_id": device_id,
                "certificate_co2_ppm": row.get("certificate_co2_ppm", ""),
                "mean_device_co2_ppm": row.get("mean_device_co2_ppm", ""),
                "device_error_ppm": row.get("device_error_ppm", ""),
                "device_error_pct": device_error_pct,
                "device_output_relative_error_limit_pct": float(device_output_relative_error_limit_pct),
                "mean_ratio_f": row.get("mean_ratio_f", ""),
                "primary_only_pred_ppm": row.get("primary_only_pred_ppm", ""),
                "primary_only_error_ppm": row.get("primary_only_error_ppm", ""),
                "primary_only_error_pct": primary_error_pct,
                "primary_model_relative_error_limit_pct": float(primary_model_relative_error_limit_pct),
                "device_output_qc": "fail" if output_failed else "pass",
                "primary_ratio_model_qc": "pass" if primary_near else "review",
                "paired_review_need": (
                    "senco1_senco3_pair_required"
                    if output_failed and primary_near
                    else "review_required"
                ),
                "primary_senco": mapped.get("primary_senco", "SENCO1"),
                "secondary_senco": mapped.get("secondary_senco", "SENCO3"),
                "secondary_action_before_verification": mapped.get("secondary_action", ""),
                "diagnosis": row.get("diagnosis", ""),
                "likely_cause": row.get("likely_cause", ""),
                "formal_acceptance_status": "blocked_not_real_acceptance",
            }
        )
    return diagnostics


def _summary_rows(
    *,
    candidate_devices: Sequence[Mapping[str, Any]],
    device_diagnostics: Sequence[Mapping[str, Any]],
    candidate_dir: Path,
    mapping_review_dir: Path,
    post_write_verification_dir: Path,
) -> List[Dict[str, Any]]:
    device_count = len(candidate_devices) or len(device_diagnostics)
    device_output_failed = sum(1 for row in device_diagnostics if row.get("device_output_qc") == "fail")
    primary_near = sum(1 for row in device_diagnostics if row.get("primary_ratio_model_qc") == "pass")
    review_status = (
        "blocked_single_senco1_write_failed_pair_review_required"
        if device_output_failed
        else "ready_for_pair_contract_review"
    )
    return [
        {
            "component": "co2",
            "review_status": review_status,
            "single_senco1_write_verification_status": "failed" if device_output_failed else "not_failed",
            "device_count": device_count,
            "device_output_failed_count": device_output_failed,
            "primary_ratio_model_near_certificate_count": primary_near,
            "candidate_dir": str(candidate_dir.resolve()),
            "mapping_review_dir": str(mapping_review_dir.resolve()),
            "post_write_verification_dir": str(post_write_verification_dir.resolve()),
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "controls_pace": False,
            "writes_coefficients": False,
            "auto_write_allowed": False,
            "database_sidecar_status": "prepared_for_import_or_audit",
            "physical_meaning": (
                "The optical CO2 ratio model predicts the standard gas, but analyzer-reported CO2 did not; "
                "therefore CO2 device output must not be accepted until SENCO1/SENCO3 pairing is reviewed or rolled back."
            ),
        }
    ]


def _contract_rows() -> List[Dict[str, Any]]:
    return [
        {
            "topic": "legacy_device_mapping",
            "contract": "CO2 primary payload is SENCO1=a0..a3(+padding); CO2 secondary payload is SENCO3=a4..a8(+padding).",
            "review_status": "confirmed_from_legacy_v1_v2_mapping",
            "calibration_meaning": "A primary-only write is not equivalent to the full historical CO2 model when secondary terms remain active.",
        },
        {
            "topic": "single_senco1_post_write_observation",
            "contract": "SENCO1-only write caused analyzer CO2 output failure while primary ratio-only offline prediction stayed near certificate.",
            "review_status": "observed_failure_requires_pair_review",
            "calibration_meaning": "The analyzer internal calculation likely combines primary and secondary groups; output ppm cannot be accepted from primary terms alone.",
        },
        {
            "topic": "pressure_and_temperature_terms",
            "contract": "Pressure P is handled by pressure-channel verification/SENCO9; contaminated sealed pressure points are not CO2 fit inputs.",
            "review_status": "v1_5_scope_boundary",
            "calibration_meaning": "CO2/H2O component fitting should use clean open-flow composition data; pressure is an input/QC channel unless clean pressure-compensation validation is added.",
        },
        {
            "topic": "formal_pair_write_condition",
            "contract": "Any SENCO1+SENCO3 write needs a full model contract, old coefficient backup, reviewer approval, controlled write, readback, and independent verification.",
            "review_status": "write_blocked_until_complete",
            "calibration_meaning": "Coefficient write is a high-risk metrological action and must be justified by traceable A-grade samples and a reproducible model.",
        },
        {
            "topic": "current_device_state",
            "contract": "Current CO2 device output is not formal usable after SENCO1-only write until paired write validation or rollback is completed.",
            "review_status": "formal_acceptance_blocked",
            "calibration_meaning": "Raw ratio frames remain useful evidence, but analyzer final CO2 ppm is not a valid acceptance result in this state.",
        },
    ]


def _candidate_option_rows(device_output_failed: bool) -> List[Dict[str, Any]]:
    preserve_status = "blocked_by_post_senco1_verification_failure" if device_output_failed else "review_only"
    return [
        {
            "option_id": "preserve_existing_senco3",
            "option_name": "Keep old SENCO3 and only use new SENCO1",
            "status": preserve_status,
            "write_allowed": False,
            "evidence_needed": "Already tested by SENCO1-only write path; post-write verification must pass before acceptance.",
            "physical_risk": "Old secondary terms can dominate or distort the analyzer internal ppm output.",
        },
        {
            "option_id": "zero_secondary_terms",
            "option_name": "Set SENCO3 secondary terms to zero",
            "status": "engineering_hypothesis_no_write_only",
            "write_allowed": False,
            "evidence_needed": "Offline replay or explicit analyzer formula proof, then controlled approval and post-write verification.",
            "physical_risk": "May remove valid T/P/secondary compensation and create hidden errors outside current conditions.",
        },
        {
            "option_id": "full_pair_multitemp_multigas_a0_a8",
            "option_name": "Fit and write SENCO1+SENCO3 as a full a0..a8 pair",
            "status": "preferred_formal_path_pending_data_and_formula_contract",
            "write_allowed": False,
            "evidence_needed": "Multi-gas, multi-temperature, holdout verification, old snapshot, reviewer approval, and write/readback plan.",
            "physical_risk": "Lowest long-term ambiguity if the formula contract is proven; still requires enough data span.",
        },
        {
            "option_id": "rollback_senco1",
            "option_name": "Rollback SENCO1 to the old snapshot",
            "status": "safety_option_requires_controlled_write_approval",
            "write_allowed": False,
            "evidence_needed": "Old GETCO snapshot, affected analyzer list, approval, controlled rollback, readback, and 900 ppm recheck.",
            "physical_risk": "Restores prior model state but does not complete the new CO2 calibration.",
        },
    ]


def _gap_rows(device_output_failed: bool) -> List[Dict[str, Any]]:
    return [
        {
            "gap_id": "post_senco1_verification",
            "status": "fail" if device_output_failed else "pass",
            "blocking_level": "P0" if device_output_failed else "none",
            "needed_next": "Do not accept device CO2 ppm; perform paired SENCO1/SENCO3 review or rollback.",
        },
        {
            "gap_id": "device_formula_contract",
            "status": "missing",
            "blocking_level": "P0",
            "needed_next": "Confirm how SENCO1 and SENCO3 are combined by the analyzer firmware before any paired write.",
        },
        {
            "gap_id": "multi_temperature_span",
            "status": "missing_or_not_bound",
            "blocking_level": "P1",
            "needed_next": "Bind multi-temperature A-grade evidence before fitting secondary T/P terms.",
        },
        {
            "gap_id": "independent_holdout_after_pair_write",
            "status": "not_run",
            "blocking_level": "P0_for_release",
            "needed_next": "After any paired write, re-run independent validation gas points before formal acceptance.",
        },
        {
            "gap_id": "database_import",
            "status": "sidecar_prepared",
            "blocking_level": "P1",
            "needed_next": "Import the review bundle into PostgreSQL when DSN/database are available.",
        },
    ]


def _database_index_rows(
    *,
    summary_rows: Sequence[Mapping[str, Any]],
    device_diagnostics: Sequence[Mapping[str, Any]],
    candidate_dir: Path,
    mapping_review_dir: Path,
    post_write_verification_dir: Path,
) -> List[Dict[str, Any]]:
    summary = dict(summary_rows[0]) if summary_rows else {}
    rows: List[Dict[str, Any]] = [
        {
            "db_table": "coefficient_candidates",
            "record_key": "co2_senco1_senco3_pair_review",
            "component": "co2",
            "analyzer_device_id": "all",
            "candidate_status": str(summary.get("review_status") or "blocked"),
            "auto_write_allowed": False,
            "source_artifact_role": "candidate_coefficient_review",
            "source_path": str(candidate_dir.resolve()),
            "metadata_json": _compact_json(summary),
        },
        {
            "db_table": "coefficient_write_events",
            "record_key": "co2_senco1_single_write_post_verification_failed",
            "component": "co2",
            "analyzer_device_id": "all",
            "candidate_status": "review_required",
            "auto_write_allowed": False,
            "source_artifact_role": "coefficient_write_log",
            "source_path": str(post_write_verification_dir.resolve()),
            "metadata_json": _compact_json(
                {
                    "event_type": "post_senco1_write_verification",
                    "status": summary.get("single_senco1_write_verification_status", ""),
                    "requires_pair_review": True,
                }
            ),
        },
        {
            "db_table": "reports",
            "record_key": "co2_senco_pair_review_report",
            "component": "co2",
            "analyzer_device_id": "all",
            "candidate_status": "available",
            "auto_write_allowed": False,
            "source_artifact_role": "candidate_coefficient_review",
            "source_path": str(mapping_review_dir.resolve()),
            "metadata_json": _compact_json({"report_type": "co2_senco_pair_review"}),
        },
    ]
    for row in device_diagnostics:
        rows.append(
            {
                "db_table": "qc_results",
                "record_key": f"co2_post_senco1_output_qc_{row.get('analyzer_device_id')}",
                "component": "co2",
                "analyzer_device_id": row.get("analyzer_device_id", ""),
                "candidate_status": str(row.get("formal_acceptance_status") or "blocked"),
                "auto_write_allowed": False,
                "source_artifact_role": "candidate_coefficient_review",
                "source_path": str(post_write_verification_dir.resolve()),
                "metadata_json": _compact_json(row),
            }
        )
    return rows


def build_co2_senco_pair_review_tables(
    *,
    candidate_dir: str | Path,
    mapping_review_dir: str | Path,
    post_write_verification_dir: str | Path,
    device_output_abs_error_limit_ppm: float = 20.0,
    primary_model_abs_error_limit_ppm: float = 10.0,
    device_output_relative_error_limit_pct: float = 1.0,
    primary_model_relative_error_limit_pct: float = 1.0,
) -> Dict[str, List[Dict[str, Any]]]:
    candidate_dir_path = Path(candidate_dir).resolve()
    mapping_review_dir_path = Path(mapping_review_dir).resolve()
    post_write_dir_path = Path(post_write_verification_dir).resolve()

    candidate_rows = _read_csv(candidate_dir_path / "candidate_coefficients.csv")
    mapping_rows = _read_csv(mapping_review_dir_path / "candidate_senco_mapping_review.csv")
    verification_rows = _read_csv(post_write_dir_path / "post_write_900ppm_primary_only_vs_device_output_summary.csv")

    candidate_devices = _candidate_devices(candidate_rows)
    device_diagnostics = _device_diagnostics(
        verification_rows=verification_rows,
        mapping_rows=mapping_rows,
        device_output_abs_error_limit_ppm=device_output_abs_error_limit_ppm,
        primary_model_abs_error_limit_ppm=primary_model_abs_error_limit_ppm,
        device_output_relative_error_limit_pct=device_output_relative_error_limit_pct,
        primary_model_relative_error_limit_pct=primary_model_relative_error_limit_pct,
    )
    summary_rows = _summary_rows(
        candidate_devices=candidate_devices,
        device_diagnostics=device_diagnostics,
        candidate_dir=candidate_dir_path,
        mapping_review_dir=mapping_review_dir_path,
        post_write_verification_dir=post_write_dir_path,
    )
    device_output_failed = any(row.get("device_output_qc") == "fail" for row in device_diagnostics)
    return {
        "co2_senco_pair_review_summary": summary_rows,
        "co2_senco_pair_contract_review": _contract_rows(),
        "co2_senco_pair_candidate_devices": candidate_devices,
        "co2_senco_pair_device_diagnostics": device_diagnostics,
        "co2_senco_pair_candidate_options": _candidate_option_rows(device_output_failed),
        "co2_senco_pair_gap_matrix": _gap_rows(device_output_failed),
        "co2_senco_pair_database_index": _database_index_rows(
            summary_rows=summary_rows,
            device_diagnostics=device_diagnostics,
            candidate_dir=candidate_dir_path,
            mapping_review_dir=mapping_review_dir_path,
            post_write_verification_dir=post_write_dir_path,
        ),
    }


def _write_database_sidecar(
    path: str | Path,
    *,
    outputs: Mapping[str, Path],
    tables: Mapping[str, Sequence[Mapping[str, Any]]],
) -> Path:
    target = Path(path).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    artifacts: List[Dict[str, Any]] = []
    for key, output_path in sorted(outputs.items()):
        if not output_path.exists():
            continue
        role = "candidate_coefficient_review" if "co2_senco_pair" in output_path.name else "evidence_file"
        artifacts.append(
            {
                "output_key": key,
                "artifact_role": role,
                "path": str(output_path.resolve()),
                "sha256": _sha256_file(output_path),
                "size_bytes": output_path.stat().st_size,
            }
        )
    payload = {
        "schema": "v1_5_co2_senco_pair_review_database_sidecar",
        "created_at": _now(),
        "no_write": True,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "database_target_tables": [
            "sample_files",
            "coefficient_candidates",
            "coefficient_write_events",
            "qc_results",
            "reports",
            "audit_events",
        ],
        "artifacts": artifacts,
        "suggested_rows": list(tables.get("co2_senco_pair_database_index", [])),
    }
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return target


def write_co2_senco_pair_review_report(
    *,
    candidate_dir: str | Path,
    mapping_review_dir: str | Path,
    post_write_verification_dir: str | Path,
    output_dir: str | Path,
    database_sidecar_json: str | Path | None = None,
    device_output_abs_error_limit_ppm: float = 20.0,
    primary_model_abs_error_limit_ppm: float = 10.0,
    device_output_relative_error_limit_pct: float = 1.0,
    primary_model_relative_error_limit_pct: float = 1.0,
) -> Dict[str, Path]:
    tables = build_co2_senco_pair_review_tables(
        candidate_dir=candidate_dir,
        mapping_review_dir=mapping_review_dir,
        post_write_verification_dir=post_write_verification_dir,
        device_output_abs_error_limit_ppm=device_output_abs_error_limit_ppm,
        primary_model_abs_error_limit_ppm=primary_model_abs_error_limit_ppm,
        device_output_relative_error_limit_pct=device_output_relative_error_limit_pct,
        primary_model_relative_error_limit_pct=primary_model_relative_error_limit_pct,
    )
    analyzers = [
        str(row.get("analyzer_device_id") or "")
        for row in tables.get("co2_senco_pair_candidate_devices", [])
        if str(row.get("analyzer_device_id") or "")
    ]
    metadata = ValidationMetadata(
        tool_name="v1_5_co2_senco_pair_review",
        analyzers=analyzers,
        input_paths=[
            str(Path(candidate_dir).resolve()),
            str(Path(mapping_review_dir).resolve()),
            str(Path(post_write_verification_dir).resolve()),
        ],
        output_dir=str(Path(output_dir).resolve()),
        config_summary={
            "device_output_abs_error_limit_ppm": float(device_output_abs_error_limit_ppm),
            "primary_model_abs_error_limit_ppm": float(primary_model_abs_error_limit_ppm),
            "device_output_relative_error_limit_pct": float(device_output_relative_error_limit_pct),
            "primary_model_relative_error_limit_pct": float(primary_model_relative_error_limit_pct),
            "no_write": True,
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
        },
        notes=[
            "SENCO1-only post-write verification failed for device CO2 output.",
            "The primary optical ratio model remains near the certificate value, so the next review is SENCO1/SENCO3 pairing.",
            "This review does not authorize a coefficient write.",
        ],
    )
    outputs = write_validation_report(
        output_dir,
        prefix="co2_senco_pair_review",
        metadata=metadata,
        tables=tables,
    )
    sidecar_path = (
        Path(database_sidecar_json).resolve()
        if database_sidecar_json
        else Path(output_dir).resolve() / "co2_senco_pair_review_database_sidecar.json"
    )
    outputs["database_sidecar"] = _write_database_sidecar(sidecar_path, outputs=outputs, tables=tables)
    return outputs
