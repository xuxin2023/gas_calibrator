"""Build an importable V1.5 evidence-registry bundle from sidecar artifacts."""

from __future__ import annotations

import csv
import hashlib
import json
import math
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from ...validation.formal_calibration_package import build_formal_calibration_package_tables
from ...validation.formal_open_flow_artifacts import load_plan_snapshot, load_pressure_reference_snapshot


TABLE_NAMES = (
    "runs",
    "devices",
    "run_devices",
    "standard_gases",
    "reference_certificates",
    "calibration_points",
    "sample_files",
    "qc_results",
    "coefficient_snapshots",
    "coefficient_candidates",
    "coefficient_write_events",
    "reports",
    "audit_events",
    "evidence_integrity_checks",
)

H2O_RAW_EVIDENCE_FIELDS = (
    "dewpoint_c|dewpoint_live_c|reference_dewpoint_c",
    "h2o_mmol_target|live_reference_h2o_mmol|nominal_reference_h2o_mmol|h2o_dry_ppmv|h2o_wet_ppmv",
    "optional:h2o_dry_ppmv",
    "optional:h2o_wet_ppmv",
    "ga*_h2o_signal",
    "ga*_h2o_ratio_raw",
    "ga*_h2o_ratio_f",
    "ga*_ref_signal",
    "ga*_h2o_mmol",
)
H2O_RAW_BASE_FIELD_GROUPS = {
    "dewpoint_reference": ("dewpoint_c", "dewpoint_live_c", "reference_dewpoint_c"),
    "h2o_reference_amount": (
        "h2o_mmol_target",
        "live_reference_h2o_mmol",
        "nominal_reference_h2o_mmol",
        "reference_h2o_mmol",
        "h2o_dry_ppmv",
        "h2o_wet_ppmv",
    ),
}
H2O_RAW_OPTIONAL_BASE_FIELDS = ("h2o_dry_ppmv", "h2o_wet_ppmv")
H2O_RAW_ANALYZER_SUFFIXES = ("h2o_signal", "h2o_ratio_raw", "h2o_ratio_f", "ref_signal", "h2o_mmol")

POST_WRITE_REVERIFICATION_EVIDENCE_ROLES = {
    "post_write_reverification_review",
    "post_write_reverification_points",
    "post_write_reverification_device_summary",
}
POST_WRITE_REVERIFICATION_COMPLETION_ROLES = {
    "post_write_reverification_points",
    "post_write_reverification_device_summary",
}
NO_WRITE_EVENT_STATUSES = {"not_attempted", "blocked"}
COEFFICIENT_WRITE_LOG_PLACEHOLDER_EVENT_TYPES = {"coefficient_write_log_present"}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def stable_id(*parts: Any) -> str:
    payload = json.dumps(parts, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_json(payload: Any) -> str:
    rendered = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(rendered.encode("utf-8")).hexdigest()


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def _iso_date_or_none(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10]).isoformat()
    except Exception:
        return None


def _split_reasons(value: Any) -> List[str]:
    if value in (None, ""):
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    return [item for item in str(value).split(";") if item]


def _bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"true", "1", "yes", "pass", "ok"}


def _clean_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    return {str(key): value for key, value in row.items()}


def _h2o_raw_field_evidence(raw_headers: Sequence[str]) -> Dict[str, Any]:
    headers = {str(item or "").strip().lower() for item in raw_headers}
    present_base: List[str] = []
    missing_base: List[str] = []
    base_fields_by_group: Dict[str, List[str]] = {}
    for group, candidates in H2O_RAW_BASE_FIELD_GROUPS.items():
        matched = [field for field in candidates if field in headers]
        base_fields_by_group[group] = matched
        if matched:
            present_base.extend(matched)
        else:
            missing_base.append("|".join(candidates))
    optional_missing_base = [field for field in H2O_RAW_OPTIONAL_BASE_FIELDS if field not in headers]
    analyzer_fields_by_suffix: Dict[str, List[str]] = {suffix: [] for suffix in H2O_RAW_ANALYZER_SUFFIXES}
    for header in sorted(headers):
        for suffix in H2O_RAW_ANALYZER_SUFFIXES:
            if header == suffix or header.endswith(f"_{suffix}"):
                analyzer_fields_by_suffix[suffix].append(header)
                break
    missing_analyzer_suffixes = [
        suffix for suffix, fields in analyzer_fields_by_suffix.items() if not fields
    ]
    present_analyzer_fields = [
        field for fields in analyzer_fields_by_suffix.values() for field in fields
    ]
    return {
        "required": list(H2O_RAW_EVIDENCE_FIELDS),
        "present": present_base + present_analyzer_fields,
        "missing": missing_base + [f"ga*_{suffix}" for suffix in missing_analyzer_suffixes],
        "present_base": present_base,
        "missing_base": missing_base,
        "base_fields_by_group": base_fields_by_group,
        "optional_missing_base": optional_missing_base,
        "present_analyzer_fields": present_analyzer_fields,
        "missing_analyzer_suffixes": missing_analyzer_suffixes,
        "analyzer_fields_by_suffix": analyzer_fields_by_suffix,
        "complete": not missing_base and not missing_analyzer_suffixes,
    }


def _primary_pressure_reference(reference: Mapping[str, Any]) -> Mapping[str, Any]:
    if isinstance(reference.get("primary"), Mapping):
        return reference["primary"]  # type: ignore[index]
    if isinstance(reference.get("com22"), Mapping):
        return reference["com22"]  # type: ignore[index]
    if isinstance(reference.get("pressure_reference"), Mapping):
        return reference["pressure_reference"]  # type: ignore[index]
    return reference


def _artifact_role(path: Path, *, plan_path: Optional[Path], pressure_reference_path: Optional[Path]) -> str:
    resolved = path.resolve()
    if plan_path is not None and resolved == plan_path.resolve():
        return "formal_plan_snapshot"
    if pressure_reference_path is not None and resolved == pressure_reference_path.resolve():
        return "pressure_reference_snapshot"
    name = path.name.lower()
    parent = path.parent.name.lower()
    path_text = str(path).replace("\\", "/").lower()
    if name.startswith("samples_") and name.endswith(".csv"):
        return "raw_samples"
    if (
        "optical_root_cause" in name
        or "光学根因" in path.name
        or "factory_signal_health" in name
        or "status_register" in name
        or "invalid_frame" in name
    ):
        return "diagnostic_analysis"
    if name.startswith("v1_5_recommendation_closure"):
        return "formal_analysis"
    if name == "pressure_channel_completion_summary.csv" or "pressure_channel_completion" in name:
        return "pressure_channel_completion"
    if "pressure_channel_quick_check" in name or "pressure_quick_check" in name:
        return "pressure_channel_quick_check"
    if "package_summary" in name:
        return "formal_package_summary"
    if (
        "candidate_coefficient_review" in name
        or "co2_senco_pair_review" in name
        or "co2_senco_pair_model_scope" in name
        or "h2o_senco24_candidate_review" in name
        or "h2o_senco24_measurement_contract" in name
    ):
        return "candidate_coefficient_review"
    if name in {"post_write_reverification_review.json", "post_write_reverification_review.md"}:
        return "post_write_reverification_review"
    if name == "post_write_reverification_points.csv":
        return "post_write_reverification_points"
    if name == "post_write_reverification_device_summary.csv":
        return "post_write_reverification_device_summary"
    if name == "post_write_reverification_plan.csv":
        return "post_run_reverification_plan"
    if "post_write_reverify" in path_text or "post_write_reverification" in path_text:
        if any(token in name for token in ("candidate", "projected_errors", "review_ready_for_write")):
            return "candidate_coefficient_review"
        if "result_summary" in name or "device_max_summary" in name:
            return "post_write_reverification_device_summary"
        if re.search(r"(?:co2|h2o)_post_write_reverify_.*\.csv$", name) or re.search(
            r"(?:co2|h2o)_after_s\d+_reverify_.*\.csv$",
            name,
        ):
            return "post_write_reverification_points"
        if (
            name == "post_write_reverify_manifest.json"
            or name.endswith("_process_record.json")
            or name.endswith("_stdout.log")
            or name.endswith("_stderr.log")
        ):
            return "post_write_reverification_review"
    if name == "evidence_bundle.json":
        return "evidence_bundle"
    if name == "evidence_bundle_integrity.json":
        return "evidence_bundle_integrity"
    if name == "v1_5_run_evidence_status.json":
        return "run_evidence_status"
    if name == "v1_5_run_evidence_status.md":
        return "run_evidence_status_report"
    if name == "v1_5_formal_run_status.json":
        return "formal_run_status"
    if name == "v1_5_formal_run_status.md":
        return "formal_run_status_report"
    if name == "v1_5_formal_run_status_gates.csv":
        return "formal_run_status_gates"
    if name == "v1_5_formal_run_status_gaps.csv":
        return "formal_run_status_gaps"
    if name == "v1_5_calibration_capability.json":
        return "calibration_capability"
    if name == "v1_5_calibration_capability.md":
        return "calibration_capability_report"
    if name == "report_model.json":
        return "report_model"
    if name == "per_device_certificate_manifest.json":
        return "per_device_certificate_manifest"
    if name == "per_device_certificate_artifact_hashes.csv":
        return "per_device_certificate_artifact_hashes"
    if name in {"queue_abort_exclusion.csv", "queue_abort_exclusion.json"}:
        return "h2o_queue_exclusion"
    if name.startswith("run_report.") and path.suffix.lower() in {".md", ".docx", ".pdf"}:
        return "run_report"
    if name.startswith("technical_report.") and path.suffix.lower() in {".md", ".docx", ".pdf"}:
        return "technical_report"
    if name.startswith("formal_calibration_report.") and path.suffix.lower() in {".md", ".docx", ".pdf"}:
        return "formal_calibration_report"
    if parent == "per_device_certificates" and "_calibration_certificate." in name:
        return "per_device_calibration_certificate"
    if parent == "per_device_certificates" and "_verification_certificate." in name:
        return "per_device_verification_certificate"
    if "controlled_write" in parent or "controlled_write" in name or "post_senco1_write" in name:
        return "coefficient_write_log"
    if "open_flow" in parent or "open_flow" in name:
        return "formal_open_flow_report"
    if "pressure_channel_validation" in parent or "pressure_validation" in name:
        return "pressure_channel_validation_report"
    if "formal_preflight" in parent or "preflight" in name:
        return "formal_preflight_report"
    if "formal_calibration_package" in parent or "formal_calibration_package" in name:
        return "formal_calibration_package"
    if "coefficient_writeback" in name:
        return "coefficient_write_log"
    if "coefficient" in name and any(token in name for token in ("before", "old", "getco", "readback")):
        return "coefficient_snapshot"
    if name.endswith(".xlsx"):
        return "workbook_report"
    if name.endswith(".json"):
        return "json_evidence"
    if name.endswith(".csv"):
        return "csv_evidence"
    return "evidence_file"


def _discover_artifact_paths(
    run_dir: Path,
    *,
    plan_path: Optional[Path],
    pressure_reference_path: Optional[Path],
    pressure_check_path: Optional[Path] = None,
    exclude_dirs: Sequence[Path] = (),
) -> List[Path]:
    extensions = {".csv", ".json", ".xlsx", ".md", ".txt", ".log", ".pdf", ".docx"}
    excluded = [path.resolve() for path in exclude_dirs]
    paths: List[Path] = []
    for path in run_dir.rglob("*"):
        resolved = path.resolve()
        if any(resolved == excluded_dir or excluded_dir in resolved.parents for excluded_dir in excluded):
            continue
        if path.name.lower() in {"evidence_bundle.json", "evidence_bundle_integrity.json"}:
            continue
        if path.is_file() and path.suffix.lower() in extensions:
            paths.append(resolved)
    for extra in (plan_path, pressure_reference_path, pressure_check_path):
        if extra is not None and extra.exists():
            paths.append(extra.resolve())
    deduped: List[Path] = []
    seen = set()
    for path in sorted(paths, key=lambda item: str(item)):
        key = str(path).lower()
        if key not in seen:
            deduped.append(path)
            seen.add(key)
    return deduped


def _required_role(role: str) -> bool:
    return role in {
        "raw_samples",
        "pressure_channel_quick_check",
        "formal_plan_snapshot",
        "pressure_reference_snapshot",
    }


def _build_file_rows(
    *,
    run_db_id: str,
    run_dir: Path,
    plan_path: Optional[Path],
    pressure_reference_path: Optional[Path],
    pressure_check_path: Optional[Path] = None,
    exclude_dirs: Sequence[Path] = (),
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for path in _discover_artifact_paths(
        run_dir,
        plan_path=plan_path,
        pressure_reference_path=pressure_reference_path,
        pressure_check_path=pressure_check_path,
        exclude_dirs=exclude_dirs,
    ):
        role = _artifact_role(path, plan_path=plan_path, pressure_reference_path=pressure_reference_path)
        stat = path.stat()
        rows.append(
            {
                "id": stable_id("sample_file", run_db_id, str(path)),
                "run_db_id": run_db_id,
                "artifact_role": role,
                "path": str(path),
                "sha256": sha256_file(path),
                "size_bytes": int(stat.st_size),
                "modified_at": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(timespec="seconds"),
                "required": _required_role(role),
                "metadata": {
                    "relative_to_run_dir": _relative_path(path, run_dir),
                    "extension": path.suffix.lower(),
                },
            }
        )
    return rows


def _relative_path(path: Path, root: Path) -> str:
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except Exception:
        return str(path.resolve())


def _artifact_id_by_role(files: Sequence[Mapping[str, Any]], role: str) -> Optional[str]:
    for row in files:
        if row.get("artifact_role") == role:
            return str(row.get("id") or "")
    return None


def _resolve_analyzer_identity(
    *,
    plan: Mapping[str, Any],
    tables: Mapping[str, Sequence[Mapping[str, Any]]],
    analyzer_prefix: str,
) -> tuple[str, str]:
    for key in ("analyzer_device_id", "analyzer_id", "device_id"):
        value = str(plan.get(key) or "").strip()
        if value:
            return value, f"plan.{key}"

    for table_name in (
        "open_flow_run_summary",
        "candidate_coefficient_review",
        "a_grade_samples",
        "b_grade_review_samples",
        "rejected_samples",
        "pressure_paired_samples",
    ):
        for row in tables.get(table_name, []) or []:
            value = str(row.get("analyzer_device_id") or "").strip()
            if value:
                return value, f"{table_name}.analyzer_device_id"

    return str(analyzer_prefix or "ga01"), "fallback_acquisition_channel"


def _resolve_analyzer_identities(
    *,
    plan: Mapping[str, Any],
    tables: Mapping[str, Sequence[Mapping[str, Any]]],
    analyzer_prefix: str,
) -> List[tuple[str, str, str]]:
    """Resolve DUT identities as sensor IDs, keeping channel labels separate.

    A V1.5 formal run can contain several analyzers sampled in parallel.  The
    database registry must index each physical sensor ID instead of collapsing
    them into the first acquisition channel.
    """

    requested = [item.strip() for item in str(analyzer_prefix or "").split(",") if item.strip()]
    all_requested = not requested or any(item.lower() == "all" for item in requested)

    plan_identity, plan_source = _resolve_analyzer_identity(
        plan=plan,
        tables=tables,
        analyzer_prefix=analyzer_prefix,
    )
    if not all_requested and len(requested) <= 1:
        return [(requested[0] if requested else str(analyzer_prefix or "ga01"), plan_identity, plan_source)]

    identities: List[tuple[str, str, str]] = []
    seen: set[tuple[str, str]] = set()
    for table_name in (
        "candidate_coefficient_review",
        "open_flow_run_summary",
        "a_grade_samples",
        "b_grade_review_samples",
        "rejected_samples",
        "pressure_paired_samples",
    ):
        for row in tables.get(table_name, []) or []:
            prefix = str(row.get("analyzer_prefix") or "").strip() or str(analyzer_prefix or "ga01")
            if not all_requested and prefix not in requested:
                continue
            device_id = str(row.get("analyzer_device_id") or "").strip()
            if not device_id:
                continue
            key = (prefix, device_id)
            if key in seen:
                continue
            seen.add(key)
            identities.append((prefix, device_id, f"{table_name}.analyzer_device_id"))

    if identities:
        return sorted(identities, key=lambda item: (item[0], item[1]))
    return [(str(analyzer_prefix or "ga01"), plan_identity, plan_source)]


def _build_devices(
    *,
    run_db_id: str,
    plan: Mapping[str, Any],
    pressure_reference: Mapping[str, Any],
    analyzer_prefix: str,
    analyzer_id: Optional[str] = None,
    analyzer_identity_source: str = "",
    analyzer_identities: Optional[Sequence[tuple[str, str, str]]] = None,
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    devices: List[Dict[str, Any]] = []
    links: List[Dict[str, Any]] = []

    identities = list(analyzer_identities or [])
    if not identities:
        analyzer_id = str(analyzer_id or plan.get("analyzer_id") or analyzer_prefix or "ga01")
        identities = [(analyzer_prefix, analyzer_id, analyzer_identity_source or "formal_plan_snapshot")]

    analyzer_device_ids: set[str] = set()
    for prefix, resolved_id, source in identities:
        if resolved_id in analyzer_device_ids:
            continue
        analyzer_device_ids.add(resolved_id)
        analyzer_row = {
            "id": stable_id("device", "analyzer", resolved_id),
            "device_type": "gas_analyzer",
            "device_role": "device_under_test",
            "display_name": resolved_id,
            "serial_number": resolved_id,
            "metadata": {
                "source": source or "formal_plan_snapshot",
                "analyzer_prefix": prefix,
                "acquisition_channel_only": source == "fallback_acquisition_channel",
            },
        }
        devices.append(analyzer_row)
        links.append(
            {
                "id": stable_id("run_device", run_db_id, analyzer_row["id"], "device_under_test"),
                "run_db_id": run_db_id,
                "device_id": analyzer_row["id"],
                "role": "device_under_test",
                "metadata": {"analyzer_prefix": prefix},
            }
        )

    primary = _primary_pressure_reference(pressure_reference)
    com22_id = str(primary.get("device_id") or "COM22")
    com22_row = {
        "id": stable_id("device", "pressure_reference", com22_id),
        "device_type": "digital_pressure_gauge",
        "device_role": "primary_pressure_reference",
        "display_name": com22_id,
        "serial_number": com22_id,
        "metadata": {"source": "pressure_reference_snapshot"},
    }
    devices.append(com22_row)
    links.append(
        {
            "id": stable_id("run_device", run_db_id, com22_row["id"], "primary_pressure_reference"),
            "run_db_id": run_db_id,
            "device_id": com22_row["id"],
            "role": "primary_pressure_reference",
            "metadata": {"reference_role": "COM22"},
        }
    )

    auxiliary = pressure_reference.get("auxiliary") if isinstance(pressure_reference, Mapping) else None
    if isinstance(auxiliary, Mapping):
        pace_id = str(auxiliary.get("device_id") or auxiliary.get("name") or "PACE")
        pace_metadata = dict(auxiliary)
    else:
        pace_id = "PACE"
        pace_metadata = {"certificate_required_for_formal_pressure": False, "role": "auxiliary_reference"}
    pace_row = {
        "id": stable_id("device", "pressure_auxiliary", pace_id),
        "device_type": "pressure_controller",
        "device_role": "auxiliary_pressure_reference",
        "display_name": pace_id,
        "serial_number": pace_id,
        "metadata": pace_metadata,
    }
    devices.append(pace_row)
    links.append(
        {
            "id": stable_id("run_device", run_db_id, pace_row["id"], "auxiliary_pressure_reference"),
            "run_db_id": run_db_id,
            "device_id": pace_row["id"],
            "role": "auxiliary_pressure_reference",
            "metadata": {"reference_role": "PACE"},
        }
    )
    return devices, links


def _build_standard_gases(run_db_id: str, plan: Mapping[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    gases = plan.get("standard_gases")
    if not isinstance(gases, Sequence) or isinstance(gases, (str, bytes)):
        return rows
    for index, gas in enumerate(gases, start=1):
        if not isinstance(gas, Mapping):
            continue
        component = str(gas.get("component") or f"gas_{index}").lower()
        cylinder_id = str(gas.get("cylinder_id") or gas.get("source_id") or f"gas_{index}")
        rows.append(
            {
                "id": stable_id("standard_gas", run_db_id, component, cylinder_id, index),
                "run_db_id": run_db_id,
                "component": component,
                "cylinder_id": cylinder_id,
                "certificate_value": _safe_float(gas.get("certificate_value")),
                "certificate_uncertainty": _safe_float(gas.get("certificate_uncertainty")),
                "valid_until": _iso_date_or_none(gas.get("valid_until")),
                "supplier": str(gas.get("supplier") or "") or None,
                "certificate_hash": str(gas.get("certificate_hash") or "") or None,
                "metadata": _clean_row(gas),
            }
        )
    return rows


def _build_reference_certificates(
    *,
    run_db_id: str,
    pressure_reference: Mapping[str, Any],
    devices: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    primary = _primary_pressure_reference(pressure_reference)
    device_id = None
    primary_device_text = str(primary.get("device_id") or "COM22")
    for row in devices:
        if row.get("device_role") == "primary_pressure_reference" and row.get("display_name") == primary_device_text:
            device_id = str(row.get("id") or "")
            break
    return [
        {
            "id": stable_id("reference_certificate", run_db_id, "primary_pressure_reference", primary.get("certificate_id")),
            "run_db_id": run_db_id,
            "device_id": device_id,
            "reference_role": "primary_pressure_reference",
            "certificate_id": str(primary.get("certificate_id") or "") or None,
            "certificate_hash": str(primary.get("certificate_hash") or "") or None,
            "valid_until": _iso_date_or_none(primary.get("valid_until")),
            "uncertainty": _safe_float(primary.get("certificate_uncertainty")),
            "unit": str(primary.get("unit") or "hPa"),
            "metadata": _clean_row(primary),
        }
    ]


def _component_from_point_row(row: Mapping[str, Any]) -> str:
    text = str(row.get("component") or row.get("route") or row.get("point_phase") or "").strip().lower()
    if text in {"co2", "气路"}:
        return "co2"
    if text in {"h2o", "water", "水路"}:
        return "h2o"
    return text or "unknown"


def _pressure_mode_from_point_row(row: Mapping[str, Any]) -> str:
    explicit = str(row.get("pressure_mode") or "").strip().lower()
    if explicit:
        return explicit
    identity_text = " ".join(
        str(row.get(key) or "").strip().lower()
        for key in ("point_tag", "point_phase", "route", "point_key", "source_point_identity")
    )
    if "open_flow" in identity_text or "开放流通" in identity_text:
        return "ambient_open"
    return ""


def _point_group_key(row: Mapping[str, Any]) -> tuple[str, str, str, str]:
    component = _component_from_point_row(row)
    point_key = str(row.get("point_row") or row.get("sample_point") or row.get("point_phase") or row.get("route") or "point")
    point_tag = str(row.get("point_tag") or row.get("point_phase") or row.get("route") or "")
    pressure_mode = _pressure_mode_from_point_row(row)
    return component, point_key, point_tag, pressure_mode


def _target_from_text(row: Mapping[str, Any], *keys: str) -> Optional[float]:
    for key in keys:
        text = str(row.get(key) or "").strip()
        if not text:
            continue
        match = re.search(r"(?<!\d)(\d+(?:\.\d+)?)\s*ppm\b", text, flags=re.IGNORECASE)
        if match:
            return _safe_float(match.group(1))
    return None


def _target_value(row: Mapping[str, Any], component: str) -> Optional[float]:
    if component == "h2o":
        keys = (
            "target_h2o_mmol",
            "h2o_mmol_target",
            "h2o_target_mmol",
            "target_value",
            "ppm_H2O_Dew",
            "certificate_value",
        )
    else:
        keys = (
            "target_co2_ppm",
            "co2_target_ppm",
            "target_ppm",
            "target_value",
            "certificate_co2_ppm",
            "ppm_CO2_Tank",
            "certificate_value",
        )
    for key in keys:
        value = _safe_float(row.get(key))
        if value is not None:
            return value
    if component == "co2":
        return _target_from_text(row, "point_tag", "point_title", "point_key", "sample_index", "source_run_id")
    return None


def _build_calibration_points(run_db_id: str, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> List[Dict[str, Any]]:
    grouped: Dict[tuple[str, str, str, str], Dict[str, Any]] = {}
    table_map = (
        ("a_grade_samples", "a_grade_count"),
        ("b_grade_review_samples", "b_grade_count"),
        ("rejected_samples", "rejected_count"),
    )
    for table_name, count_key in table_map:
        for row in tables.get(table_name, []):
            key = _point_group_key(row)
            item = grouped.setdefault(
                key,
                {
                    "component": key[0],
                    "point_key": key[1],
                    "point_tag": key[2],
                    "pressure_mode": key[3],
                    "target_value": _target_value(row, key[0]),
                    "sample_count": 0,
                    "a_grade_count": 0,
                    "b_grade_count": 0,
                    "rejected_count": 0,
                    "metadata": {},
                },
            )
            item["sample_count"] += 1
            item[count_key] += 1
            if item["target_value"] is None:
                item["target_value"] = _target_value(row, key[0])

    rows: List[Dict[str, Any]] = []
    for key, item in sorted(grouped.items(), key=lambda entry: entry[0]):
        rows.append(
            {
                "id": stable_id("calibration_point", run_db_id, *key),
                "run_db_id": run_db_id,
                **item,
            }
        )
    return rows


def _build_qc_rows(
    *,
    run_db_id: str,
    tables: Mapping[str, Sequence[Mapping[str, Any]]],
    source_artifacts: Mapping[str, Optional[str]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for row in tables.get("package_summary", []):
        status = str(row.get("package_status") or "")
        rows.append(
            {
                "id": stable_id("qc", run_db_id, "formal_package_status"),
                "run_db_id": run_db_id,
                "scope": "run",
                "subject_id": None,
                "rule_name": "formal_package_status",
                "status": status,
                "severity": "error" if status != "ready_for_reviewer" else "info",
                "reasons": _split_reasons(row.get("package_blockers")),
                "metrics": _clean_row(row),
                "source_artifact_id": source_artifacts.get("formal_calibration_package"),
                "metadata": {"physical_scope": "evidence_package"},
            }
        )

    for row in tables.get("open_flow_run_summary", []):
        component = str(row.get("component") or "")
        analyzer_key = str(row.get("analyzer_device_id") or row.get("analyzer_prefix") or component)
        status = "pass" if _bool_value(row.get("candidate_fit_allowed")) else "fail"
        rows.append(
            {
                "id": stable_id("qc", run_db_id, "open_flow_qc", component, analyzer_key),
                "run_db_id": run_db_id,
                "scope": "component",
                "subject_id": component,
                "rule_name": "open_flow_qc_classification",
                "status": status,
                "severity": "error" if status != "pass" else "info",
                "reasons": _split_reasons(row.get("candidate_fit_blockers")),
                "metrics": _clean_row(row),
                "source_artifact_id": source_artifacts.get("formal_open_flow_report"),
                "metadata": {
                    "physical_scope": "open_flow_component_stability",
                    "analyzer_device_id": analyzer_key,
                },
            }
        )

    for row in tables.get("pressure_validation_summary", []):
        status = str(row.get("status") or "")
        analyzer_key = str(row.get("analyzer_device_id") or row.get("analyzer_prefix") or "analyzer_internal_pressure_P")
        rows.append(
            {
                "id": stable_id("qc", run_db_id, "pressure_channel_ambient_quick_check", analyzer_key),
                "run_db_id": run_db_id,
                "scope": "pressure_channel",
                "subject_id": analyzer_key,
                "rule_name": "pressure_channel_ambient_quick_check",
                "status": status,
                "severity": "error" if status != "pass" else "info",
                "reasons": _split_reasons(row.get("reason")),
                "metrics": _clean_row(row),
                "source_artifact_id": source_artifacts.get("pressure_channel_quick_check"),
                "metadata": {"physical_scope": "pressure_input_validation"},
            }
        )

    seen_pressure_reference_qc = set()
    for row in tables.get("pressure_reference_traceability", []):
        status = str(row.get("status") or "")
        subject_id = str(row.get("device_id") or "COM22")
        dedupe_key = (
            subject_id,
            "pressure_reference_traceability",
            status,
            json.dumps(_clean_row(row), ensure_ascii=False, sort_keys=True, default=str),
        )
        if dedupe_key in seen_pressure_reference_qc:
            continue
        seen_pressure_reference_qc.add(dedupe_key)
        rows.append(
            {
                "id": stable_id("qc", run_db_id, "pressure_reference_traceability", subject_id),
                "run_db_id": run_db_id,
                "scope": "traceability",
                "subject_id": subject_id,
                "rule_name": "pressure_reference_traceability",
                "status": status,
                "severity": "error" if status != "pass" else "info",
                "reasons": _split_reasons(row.get("reasons")),
                "metrics": _clean_row(row),
                "source_artifact_id": source_artifacts.get("pressure_reference_snapshot"),
                "metadata": {
                    "physical_scope": "COM22_certificate",
                    "deduped_shared_reference": True,
                    "source_row_count": len(tables.get("pressure_reference_traceability", [])),
                },
            }
        )
    return rows


def _build_candidates(
    *,
    run_db_id: str,
    tables: Mapping[str, Sequence[Mapping[str, Any]]],
    candidate_artifact_id: Optional[str],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for row in tables.get("candidate_coefficient_review", []):
        component = str(row.get("component") or "")
        analyzer_key = str(row.get("analyzer_device_id") or row.get("analyzer_prefix") or "")
        rows.append(
            {
                "id": stable_id("coefficient_candidate", run_db_id, component, analyzer_key),
                "run_db_id": run_db_id,
                "component": component,
                "candidate_status": str(row.get("candidate_review_status") or "blocked"),
                "allowed_for_review": _bool_value(row.get("candidate_fit_may_be_reviewed")),
                "auto_write_allowed": _bool_value(row.get("candidate_fit_auto_write_allowed")),
                "blockers": _split_reasons(row.get("blockers")),
                "coefficients": {},
                "source_artifact_id": candidate_artifact_id,
                "metadata": _clean_row(row),
            }
        )
    return rows


def _read_small_json(path: Path) -> Optional[Any]:
    if path.stat().st_size > 1024 * 1024:
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return None


def _build_coefficient_snapshots(
    *,
    run_db_id: str,
    analyzer_id: str,
    files: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for file_row in files:
        if file_row.get("artifact_role") != "coefficient_snapshot":
            continue
        path = Path(str(file_row.get("path") or ""))
        payload = _read_small_json(path) if path.suffix.lower() == ".json" and path.exists() else None
        coefficients = payload if isinstance(payload, Mapping) else {}
        device_entries: List[tuple[str, Mapping[str, Any]]] = []
        if isinstance(coefficients, Mapping):
            for key, value in coefficients.items():
                if not isinstance(value, Mapping):
                    continue
                has_getco_payload = any(str(item).startswith("GETCO") for item in value)
                if value.get("analyzer_device_id") or has_getco_payload:
                    device_entries.append((str(value.get("analyzer_device_id") or key), value))
        if not device_entries:
            device_entries = [(analyzer_id, coefficients if isinstance(coefficients, Mapping) else {})]
        for entry_analyzer_id, entry_coefficients in device_entries:
            snapshot_type = "old_component_getco_coefficients" if "old" in path.name.lower() else "readback_coefficients"
            getco_groups = sorted(
                {
                    key.replace("_before", "").replace("_after", "")
                    for key in entry_coefficients
                    if str(key).startswith("GETCO") and (str(key).endswith("_before") or str(key).endswith("_after"))
                }
            )
            rows.append(
                {
                    "id": stable_id("coefficient_snapshot", run_db_id, str(path), entry_analyzer_id),
                    "run_db_id": run_db_id,
                    "analyzer_id": entry_analyzer_id,
                    "snapshot_type": snapshot_type,
                    "coefficients": dict(entry_coefficients),
                    "coefficients_hash": (
                        sha256_json(entry_coefficients) if entry_coefficients else str(file_row.get("sha256") or "")
                    ),
                    "source_artifact_id": str(file_row.get("id") or ""),
                    "metadata": {
                        "path": str(path),
                        "artifact_sha256": file_row.get("sha256"),
                        "snapshot_file_name": path.name,
                        "getco_groups": getco_groups,
                    },
                }
            )
    return rows


def _build_write_events(
    *,
    run_db_id: str,
    analyzer_id: str,
    files: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    rows = [
        {
            "id": stable_id("coefficient_write_event", run_db_id, "sidecar_import_no_write"),
            "run_db_id": run_db_id,
            "analyzer_id": analyzer_id,
            "event_type": "sidecar_import_no_write",
            "status": "not_attempted",
            "approved_by": None,
            "command_summary": "No device write is performed by the evidence-registry importer.",
            "old_coefficients_hash": None,
            "candidate_id": None,
            "readback": {},
            "metadata": {
                "formal_boundary": "candidate review only",
                "device_write_allowed": False,
                "opens_com_ports": False,
            },
        }
    ]
    for file_row in files:
        if file_row.get("artifact_role") != "coefficient_write_log":
            continue
        rows.append(
            {
                "id": stable_id("coefficient_write_event", run_db_id, str(file_row.get("path"))),
                "run_db_id": run_db_id,
                "analyzer_id": analyzer_id,
                "event_type": "coefficient_write_log_present",
                "status": "review_required",
                "approved_by": None,
                "command_summary": "Coefficient writeback artifact exists and requires reviewer audit.",
                "old_coefficients_hash": None,
                "candidate_id": None,
                "readback": {},
                "metadata": {"artifact_id": file_row.get("id"), "path": file_row.get("path")},
            }
        )
    return rows


def _build_reports(run_db_id: str, files: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    report_roles = {
        "formal_open_flow_report",
        "pressure_channel_validation_report",
        "formal_preflight_report",
        "formal_calibration_package",
        "formal_package_summary",
        "candidate_coefficient_review",
        "post_write_reverification_review",
        "post_write_reverification_device_summary",
        "run_evidence_status",
        "run_evidence_status_report",
        "report_model",
        "run_report",
        "technical_report",
        "formal_calibration_report",
        "per_device_certificate_manifest",
        "per_device_certificate_artifact_hashes",
        "per_device_calibration_certificate",
        "per_device_verification_certificate",
        "workbook_report",
    }
    rows: List[Dict[str, Any]] = []
    for file_row in files:
        role = str(file_row.get("artifact_role") or "")
        if role not in report_roles:
            continue
        rows.append(
            {
                "id": stable_id("report", run_db_id, file_row.get("path")),
                "run_db_id": run_db_id,
                "report_type": role,
                "path": str(file_row.get("path") or ""),
                "sha256": str(file_row.get("sha256") or ""),
                "status": "available",
                "generated_at": file_row.get("modified_at"),
                "metadata": {"source_artifact_id": file_row.get("id")},
            }
        )
    return rows


def _write_event_metadata(row: Mapping[str, Any]) -> Dict[str, Any]:
    raw = row.get("metadata")
    if isinstance(raw, Mapping):
        return dict(raw)
    if isinstance(raw, str) and raw.strip():
        try:
            payload = json.loads(raw)
        except Exception:
            return {}
        if isinstance(payload, Mapping):
            return dict(payload)
    return {}


def _write_event_readback(row: Mapping[str, Any]) -> Dict[str, Any]:
    raw = row.get("readback")
    if isinstance(raw, Mapping):
        return dict(raw)
    if isinstance(raw, str) and raw.strip():
        try:
            payload = json.loads(raw)
        except Exception:
            return {}
        if isinstance(payload, Mapping):
            return dict(payload)
    metadata = _write_event_metadata(row)
    raw = metadata.get("readback")
    if isinstance(raw, Mapping):
        return dict(raw)
    return {}


def _write_event_status(row: Mapping[str, Any]) -> str:
    return str(row.get("status") or "").strip()


def _write_event_type(row: Mapping[str, Any]) -> str:
    return str(row.get("event_type") or "").strip()


def _is_no_write_event(row: Mapping[str, Any]) -> bool:
    return _write_event_status(row) in NO_WRITE_EVENT_STATUSES


def _is_write_log_placeholder(row: Mapping[str, Any]) -> bool:
    return _write_event_type(row) in COEFFICIENT_WRITE_LOG_PLACEHOLDER_EVENT_TYPES


def _is_verified_write_event(row: Mapping[str, Any]) -> bool:
    status = _write_event_status(row).lower()
    if "verified" not in status:
        return False
    readback = _write_event_readback(row)
    old_hash = str(row.get("old_coefficients_hash") or "").strip()
    metadata = _write_event_metadata(row)
    metadata_old_hash = str(metadata.get("old_coefficients_hash") or "").strip()
    return bool(readback or old_hash or metadata_old_hash)


def _coefficient_write_policy(
    *,
    coefficient_snapshots: Sequence[Mapping[str, Any]],
    write_events: Sequence[Mapping[str, Any]],
    has_post_write_reverification: bool,
) -> Dict[str, Any]:
    """Interpret coefficient-write evidence as no-write or controlled-write.

    Write-log placeholder rows only prove that a write artifact exists; they do
    not by themselves prove a controlled write. A controlled write is traceable
    only when the old coefficients, verified/readback write events, and
    post-write reverification evidence are all present.
    """

    write_like_events = [row for row in write_events if not _is_no_write_event(row)]
    placeholders = [row for row in write_like_events if _is_write_log_placeholder(row)]
    actual_write_events = [row for row in write_like_events if not _is_write_log_placeholder(row)]
    verified_write_events = [row for row in actual_write_events if _is_verified_write_event(row)]
    unverified_write_events = [row for row in actual_write_events if not _is_verified_write_event(row)]
    has_write_evidence = bool(write_like_events)
    has_old_snapshots = bool(coefficient_snapshots)
    missing_requirements: List[str] = []
    mode = "no_write"
    status = "no_write_traceable"
    interpretation = "No coefficient-write evidence is present; the run remains a no-write evidence package."

    if has_write_evidence:
        mode = "controlled_write"
        if not verified_write_events:
            missing_requirements.append("verified_write_event_with_readback")
        if not has_old_snapshots:
            missing_requirements.append("old_coefficients_snapshot")
        if not has_post_write_reverification:
            missing_requirements.append("post_write_reverification_completion")
        if unverified_write_events:
            missing_requirements.append("unverified_write_event_review")
        if missing_requirements:
            status = "controlled_write_incomplete"
            interpretation = (
                "Coefficient-write artifacts exist, but they are not yet a complete controlled-write evidence "
                "chain. Old coefficients, verified write/readback rows, and post-write reverification are all "
                "required before reviewer release."
            )
        else:
            status = "controlled_write_traceable"
            interpretation = (
                "Coefficient writes are traceable: old coefficients are snapshotted, write/readback rows are "
                "verified, and post-write reverification evidence is present."
            )

    return {
        "mode": mode,
        "status": status,
        "policy_satisfied": not missing_requirements,
        "write_attempt_count": len(write_like_events),
        "verified_write_event_count": len(verified_write_events),
        "write_log_placeholder_count": len(placeholders),
        "unverified_write_event_count": len(unverified_write_events),
        "has_old_coefficient_snapshots": has_old_snapshots,
        "has_verified_write_events": bool(verified_write_events),
        "has_post_write_reverification": bool(has_post_write_reverification),
        "missing_requirements": missing_requirements,
        "event_statuses": [_write_event_status(row) for row in write_events],
        "event_types": [_write_event_type(row) for row in write_events],
        "interpretation": interpretation,
    }


def _build_integrity_checks(
    *,
    run_db_id: str,
    context: Mapping[str, Any],
    files: Sequence[Mapping[str, Any]],
    candidates: Sequence[Mapping[str, Any]],
    coefficient_snapshots: Sequence[Mapping[str, Any]],
    write_events: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    roles = {str(row.get("artifact_role") or "") for row in files}
    required = [row for row in files if bool(row.get("required"))]
    pressure_input_present = bool(roles & {"pressure_channel_quick_check", "pressure_channel_completion"})
    has_post_write_reverification = bool(roles & POST_WRITE_REVERIFICATION_COMPLETION_ROLES)
    write_policy = _coefficient_write_policy(
        coefficient_snapshots=coefficient_snapshots,
        write_events=write_events,
        has_post_write_reverification=has_post_write_reverification,
    )
    missing_required_roles = [
        role
        for role in ("raw_samples", "formal_plan_snapshot", "pressure_reference_snapshot")
        if role not in roles
    ]
    if not pressure_input_present:
        missing_required_roles.append("pressure_channel_quick_check_or_completion")
    checks = [
        (
            "required_artifacts_hashed",
            "pass" if required and all(row.get("sha256") for row in required) else "fail",
            "error",
            {"required_count": len(required), "missing_roles": missing_required_roles},
        ),
        (
            "pressure_quick_check_artifact_present",
            "pass" if pressure_input_present else "fail",
            "error",
            {
                "pressure_check_source": context.get("pressure_check_source", ""),
                "accepted_roles": sorted(roles & {"pressure_channel_quick_check", "pressure_channel_completion"}),
            },
        ),
        (
            "formal_package_ready_for_reviewer",
            "pass" if context.get("package_status") == "ready_for_reviewer" else "fail",
            "error",
            {"package_status": context.get("package_status"), "package_blockers": context.get("package_blockers", [])},
        ),
        (
            "candidate_auto_write_disabled",
            "pass" if all(not bool(row.get("auto_write_allowed")) for row in candidates) else "fail",
            "error",
            {"candidate_count": len(candidates)},
        ),
        (
            "old_coefficients_snapshot_present",
            "pass" if coefficient_snapshots else "warn",
            "warning",
            {
                "snapshot_count": len(coefficient_snapshots),
                "note": "GETCO/old-coefficient snapshot should be attached before coefficient approval.",
            },
        ),
        (
            "coefficient_write_not_attempted",
            "pass" if write_policy["write_attempt_count"] == 0 else "warn",
            "warning",
            {
                "event_statuses": write_policy["event_statuses"],
                "note": "A controlled-write package may contain write evidence; see coefficient_write_policy_satisfied.",
            },
        ),
        (
            "coefficient_write_policy_satisfied",
            "pass" if write_policy["policy_satisfied"] else "fail",
            "error",
            write_policy,
        ),
    ]
    return [
        {
            "id": stable_id("integrity_check", run_db_id, name),
            "run_db_id": run_db_id,
            "check_name": name,
            "status": status,
            "severity": severity,
            "details": details,
        }
        for name, status, severity, details in checks
    ]


def _build_audit_events(
    *,
    run_db_id: str,
    actor: str,
    files: Sequence[Mapping[str, Any]],
    context: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    return [
        {
            "id": stable_id("audit", run_db_id, "evidence_bundle_built", sha256_json(context)),
            "run_db_id": run_db_id,
            "event_type": "evidence_bundle_built",
            "actor": actor or None,
            "event_at": _now_iso(),
            "payload": {
                "artifact_count": len(files),
                "package_status": context.get("package_status"),
                "pressure_check_source": context.get("pressure_check_source"),
                "sidecar_only": True,
                "opens_com_ports": False,
                "device_write_allowed": False,
            },
        }
    ]


def _source_artifact_map(files: Sequence[Mapping[str, Any]]) -> Dict[str, Optional[str]]:
    return {
        "formal_calibration_package": _artifact_id_by_role(files, "formal_calibration_package")
        or _artifact_id_by_role(files, "formal_package_summary"),
        "formal_open_flow_report": _artifact_id_by_role(files, "formal_open_flow_report"),
        "pressure_channel_quick_check": _artifact_id_by_role(files, "pressure_channel_quick_check")
        or _artifact_id_by_role(files, "pressure_channel_completion"),
        "pressure_reference_snapshot": _artifact_id_by_role(files, "pressure_reference_snapshot"),
        "candidate_coefficient_review": _artifact_id_by_role(files, "candidate_coefficient_review"),
    }


def _load_coefficient_write_csv_rows(files: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for file_row in files:
        if file_row.get("artifact_role") != "coefficient_write_log":
            continue
        path = Path(str(file_row.get("path") or ""))
        if not path.exists():
            continue
        try:
            with path.open("r", encoding="utf-8-sig", newline="") as handle:
                rows.extend(dict(row) for row in csv.DictReader(handle))
        except Exception:
            continue
    return rows


def _load_database_sidecar_rows(files: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for file_row in files:
        path = Path(str(file_row.get("path") or ""))
        if not path.exists() or path.suffix.lower() != ".json":
            continue
        if "database_sidecar" not in path.name.lower():
            continue
        payload = _read_small_json(path)
        if not isinstance(payload, Mapping):
            continue
        suggested = payload.get("suggested_rows")
        if not isinstance(suggested, Sequence) or isinstance(suggested, (str, bytes)):
            continue
        for item in suggested:
            if not isinstance(item, Mapping):
                continue
            row = dict(item)
            row["_source_artifact_id"] = file_row.get("id")
            row["_source_path"] = str(path)
            rows.append(row)
    return rows


def _sidecar_rows_for_table(sidecar_rows: Sequence[Mapping[str, Any]], table: str) -> List[Mapping[str, Any]]:
    return [row for row in sidecar_rows if str(row.get("db_table") or "") == table]


def _sidecar_metadata(row: Mapping[str, Any]) -> Dict[str, Any]:
    raw = row.get("metadata_json")
    if isinstance(raw, Mapping):
        return dict(raw)
    if isinstance(raw, str) and raw.strip():
        try:
            payload = json.loads(raw)
        except Exception:
            return {}
        if isinstance(payload, Mapping):
            return dict(payload)
    return {}


def _sidecar_reason_list(row: Mapping[str, Any], *keys: str) -> List[str]:
    metadata = _sidecar_metadata(row)
    for key in keys:
        reasons = _split_reasons(row.get(key))
        if reasons:
            return reasons
        reasons = _split_reasons(metadata.get(key))
        if reasons:
            return reasons
    return []


def _build_sidecar_candidates(
    *,
    run_db_id: str,
    sidecar_rows: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for row in _sidecar_rows_for_table(sidecar_rows, "coefficient_candidates"):
        component = str(row.get("component") or "")
        record_key = str(row.get("record_key") or component or "sidecar_candidate")
        status = str(row.get("candidate_status") or "blocked")
        blocked_reasons = _sidecar_reason_list(row, "blocked_reasons", "blockers")
        warning_reasons = _sidecar_reason_list(row, "warning_reasons", "warnings")
        metadata = _clean_row(row)
        metadata["blocked_reasons_list"] = blocked_reasons
        metadata["warning_reasons_list"] = warning_reasons
        if component.lower() == "h2o":
            blockers = list(blocked_reasons)
            if status == "blocked" and not blockers:
                blockers = ["h2o_sidecar_candidate_blocked_without_explicit_reason"]
        elif "model_scope" in record_key:
            blockers = [
                "co2_senco3_secondary_terms_not_identifiable",
                "formula_contract_and_secondary_span_required",
            ]
        else:
            blockers = [
                "co2_senco_pair_review_required",
                "single_senco1_write_post_verification_failed",
            ]
        rows.append(
            {
                "id": stable_id("coefficient_candidate", run_db_id, "sidecar", record_key),
                "run_db_id": run_db_id,
                "component": component,
                "candidate_status": status,
                "allowed_for_review": _bool_value(row.get("allowed_for_review") or row.get("candidate_fit_may_be_reviewed")),
                "auto_write_allowed": _bool_value(row.get("auto_write_allowed")),
                "blockers": blockers,
                "coefficients": {},
                "source_artifact_id": row.get("_source_artifact_id"),
                "metadata": metadata,
            }
        )
    return rows


def _build_sidecar_write_events(
    *,
    run_db_id: str,
    analyzer_id: str,
    sidecar_rows: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for row in _sidecar_rows_for_table(sidecar_rows, "coefficient_write_events"):
        record_key = str(row.get("record_key") or "sidecar_write_event")
        metadata = _sidecar_metadata(row)
        readback_raw = row.get("readback_json")
        if not readback_raw:
            readback_raw = metadata.get("readback")
        readback: Dict[str, Any] = {}
        if isinstance(readback_raw, Mapping):
            readback = dict(readback_raw)
        elif isinstance(readback_raw, str) and readback_raw.strip():
            try:
                parsed = json.loads(readback_raw)
            except Exception:
                parsed = {}
            if isinstance(parsed, Mapping):
                readback = dict(parsed)
        event_type = str(row.get("event_type") or metadata.get("event_type") or "").strip()
        if not event_type:
            event_type = "co2_senco1_single_write_post_verification"
        status = str(row.get("status") or row.get("candidate_status") or metadata.get("status") or "review_required")
        command_summary = str(row.get("command_summary") or metadata.get("command_summary") or "").strip()
        if not command_summary:
            command_summary = (
                "SENCO1-only write evidence exists; post-write verification failed and requires SENCO1/SENCO3 pair review."
            )
        rows.append(
            {
                "id": stable_id("coefficient_write_event", run_db_id, "sidecar", record_key),
                "run_db_id": run_db_id,
                "analyzer_id": str(row.get("analyzer_device_id") or analyzer_id),
                "event_type": event_type,
                "status": status,
                "approved_by": row.get("approved_by") or metadata.get("approved_by"),
                "command_summary": command_summary,
                "old_coefficients_hash": row.get("old_coefficients_hash") or metadata.get("old_coefficients_hash"),
                "candidate_id": row.get("candidate_id") or metadata.get("candidate_id"),
                "readback": readback,
                "metadata": _clean_row(row),
            }
        )
    return rows


def _build_sidecar_qc_rows(
    *,
    run_db_id: str,
    sidecar_rows: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for row in _sidecar_rows_for_table(sidecar_rows, "qc_results"):
        metadata = _sidecar_metadata(row)
        subject_id = str(row.get("analyzer_device_id") or "")
        record_key = str(row.get("record_key") or subject_id or "sidecar_qc")
        status_text = str(row.get("candidate_status") or "")
        status = str(metadata.get("status") or "")
        if not status:
            status = "fail" if "blocked" in status_text or "fail" in status_text else "review"
        rule_name = str(metadata.get("rule_name") or "")
        if not rule_name:
            rule_name = "co2_post_senco1_device_output_qc"
        reason = str(metadata.get("reason") or "")
        rows.append(
            {
                "id": stable_id("qc", run_db_id, "sidecar", record_key),
                "run_db_id": run_db_id,
                "scope": "co2_senco_pair_review",
                "subject_id": subject_id or None,
                "rule_name": rule_name,
                "status": status,
                "severity": "error" if status == "fail" else "warning",
                "reasons": [reason] if reason else ["device_output_failed_after_senco1_only_write"],
                "metrics": _clean_row(row),
                "source_artifact_id": row.get("_source_artifact_id"),
                "metadata": {"physical_scope": "co2_device_output_after_senco1_write"},
            }
        )
    return rows


def _run_evidence_status(context: Mapping[str, Any], checks: Sequence[Mapping[str, Any]]) -> str:
    if any(row.get("status") == "fail" for row in checks):
        return "blocked"
    if context.get("package_status") == "ready_for_reviewer":
        return "ready_for_reviewer"
    return "indexed"


def build_evidence_bundle(
    *,
    run_dir: str | Path,
    plan_path: str | Path,
    pressure_reference_path: str | Path,
    component: str = "both",
    analyzer_prefix: str = "ga01",
    require_quick_check_artifact: bool = True,
    pressure_check_path: str | Path | None = None,
    today: Any = None,
    artifact_exclude_dirs: Sequence[str | Path] = (),
) -> Dict[str, Any]:
    """Build a database-ready evidence bundle from existing V1.5 artifacts."""

    root = Path(run_dir).resolve()
    plan_file = Path(plan_path).resolve()
    reference_file = Path(pressure_reference_path).resolve()
    pressure_check_file = Path(pressure_check_path).resolve() if pressure_check_path else None
    exclude_dirs = [Path(path).resolve() for path in artifact_exclude_dirs]
    plan = load_plan_snapshot(plan_file)
    pressure_reference = load_pressure_reference_snapshot(reference_file)
    tables, context = build_formal_calibration_package_tables(
        run_dir=root,
        plan=plan,
        pressure_reference=pressure_reference,
        component=component,
        analyzer_prefix=analyzer_prefix,
        require_quick_check_artifact=require_quick_check_artifact,
        pressure_check_path=pressure_check_file,
        today=today,
    )
    run_id = root.name
    run_db_id = stable_id("run", str(root), str(plan.get("plan_id") or ""), run_id)
    analyzer_id, analyzer_identity_source = _resolve_analyzer_identity(
        plan=plan,
        tables=tables,
        analyzer_prefix=analyzer_prefix,
    )
    analyzer_identities = _resolve_analyzer_identities(
        plan=plan,
        tables=tables,
        analyzer_prefix=analyzer_prefix,
    )
    analyzer_ids = [item[1] for item in analyzer_identities]
    run_analyzer_id = analyzer_id if len(analyzer_ids) == 1 else ";".join(analyzer_ids)
    files = _build_file_rows(
        run_db_id=run_db_id,
        run_dir=root,
        plan_path=plan_file,
        pressure_reference_path=reference_file,
        pressure_check_path=pressure_check_file,
        exclude_dirs=exclude_dirs,
    )
    source_artifacts = _source_artifact_map(files)
    devices, run_devices = _build_devices(
        run_db_id=run_db_id,
        plan=plan,
        pressure_reference=pressure_reference,
        analyzer_prefix=analyzer_prefix,
        analyzer_id=analyzer_id,
        analyzer_identity_source=analyzer_identity_source,
        analyzer_identities=analyzer_identities,
    )
    candidates = _build_candidates(
        run_db_id=run_db_id,
        tables=tables,
        candidate_artifact_id=source_artifacts.get("candidate_coefficient_review"),
    )
    snapshots = _build_coefficient_snapshots(
        run_db_id=run_db_id,
        analyzer_id=run_analyzer_id,
        files=files,
    )
    write_events = _build_write_events(
        run_db_id=run_db_id,
        analyzer_id=run_analyzer_id,
        files=files,
    )
    # Loading the rows keeps the write-log artifact visible for later audit
    # without treating it as authorization to write.
    write_log_rows = _load_coefficient_write_csv_rows(files)
    if write_log_rows:
        write_events[0]["metadata"]["coefficient_write_log_rows"] = len(write_log_rows)
    database_sidecar_rows = _load_database_sidecar_rows(files)
    candidates.extend(_build_sidecar_candidates(run_db_id=run_db_id, sidecar_rows=database_sidecar_rows))
    write_events.extend(
        _build_sidecar_write_events(
            run_db_id=run_db_id,
            analyzer_id=run_analyzer_id,
            sidecar_rows=database_sidecar_rows,
        )
    )

    checks = _build_integrity_checks(
        run_db_id=run_db_id,
        context=context,
        files=files,
        candidates=candidates,
        coefficient_snapshots=snapshots,
        write_events=write_events,
    )
    actor = str(plan.get("operator") or "")
    run_row = {
        "id": run_db_id,
        "run_id": run_id,
        "run_dir": str(root),
        "plan_id": str(plan.get("plan_id") or "") or None,
        "plan_version": str(plan.get("plan_version") or "") or None,
        "analyzer_id": run_analyzer_id,
        "operator_name": actor or None,
        "config_hash": str(plan.get("config_hash") or "") or None,
        "package_status": str(context.get("package_status") or ""),
        "package_blockers": list(context.get("package_blockers") or []),
        "evidence_status": _run_evidence_status(context, checks),
        "metadata": {
            "component": component,
            "analyzer_prefix": analyzer_prefix,
            "analyzer_device_ids": analyzer_ids,
            "pressure_check_source": context.get("pressure_check_source", ""),
            "pressure_check_path": context.get("pressure_check_path", ""),
            "sidecar_only": True,
            "opens_com_ports": False,
            "device_write_allowed": False,
        },
    }
    bundle = {
        "schema": "v1_5_evidence_registry",
        "schema_version": "001",
        "created_at": _now_iso(),
        "run_db_id": run_db_id,
        "run_id": run_id,
        "tables": {
            "runs": [run_row],
            "devices": devices,
            "run_devices": run_devices,
            "standard_gases": _build_standard_gases(run_db_id, plan),
            "reference_certificates": _build_reference_certificates(
                run_db_id=run_db_id,
                pressure_reference=pressure_reference,
                devices=devices,
            ),
            "calibration_points": _build_calibration_points(run_db_id, tables),
            "sample_files": files,
            "qc_results": _build_qc_rows(
                run_db_id=run_db_id,
                tables=tables,
                source_artifacts=source_artifacts,
            )
            + _build_sidecar_qc_rows(run_db_id=run_db_id, sidecar_rows=database_sidecar_rows),
            "coefficient_snapshots": snapshots,
            "coefficient_candidates": candidates,
            "coefficient_write_events": write_events,
            "reports": _build_reports(run_db_id, files),
            "audit_events": _build_audit_events(
                run_db_id=run_db_id,
                actor=actor,
                files=files,
                context=context,
            ),
            "evidence_integrity_checks": checks,
        },
        "validation_tables": {
            name: [dict(row) for row in tables.get(name, []) if isinstance(row, Mapping)]
            for name in (
                "a_grade_samples",
                "rejected_samples",
                "pressure_validation_summary",
                "open_flow_run_summary",
            )
        },
    }
    return bundle


def bundle_summary(bundle: Mapping[str, Any]) -> Dict[str, Any]:
    tables = bundle.get("tables") if isinstance(bundle.get("tables"), Mapping) else {}
    run_row = (tables.get("runs") or [{}])[0] if isinstance(tables, Mapping) else {}
    sample_files = _table_rows(tables, "sample_files") if isinstance(tables, Mapping) else []
    status_artifacts = [
        row
        for row in sample_files
        if str(row.get("artifact_role") or "").startswith("run_evidence_status")
    ]
    status_reports = [
        row
        for row in _table_rows(tables, "reports")
        if str(row.get("report_type") or "").startswith("run_evidence_status")
    ]
    formal_status_artifacts = [
        row
        for row in sample_files
        if str(row.get("artifact_role") or "").startswith("formal_run_status")
    ]
    return {
        "schema": bundle.get("schema"),
        "schema_version": bundle.get("schema_version"),
        "run_id": bundle.get("run_id"),
        "run_db_id": bundle.get("run_db_id"),
        "evidence_status": run_row.get("evidence_status"),
        "package_status": run_row.get("package_status"),
        "table_counts": {
            name: len(tables.get(name) or [])
            for name in TABLE_NAMES
        },
        "run_evidence_status": {
            "present": bool(status_artifacts),
            "artifact_count": len(status_artifacts),
            "report_row_count": len(status_reports),
            "roles": sorted({str(row.get("artifact_role") or "") for row in status_artifacts}),
            "all_hashed": bool(status_artifacts) and all(str(row.get("sha256") or "") for row in status_artifacts),
        },
        "formal_run_status": {
            "present": bool(formal_status_artifacts),
            "artifact_count": len(formal_status_artifacts),
            "roles": sorted({str(row.get("artifact_role") or "") for row in formal_status_artifacts}),
            "all_hashed": bool(formal_status_artifacts)
            and all(str(row.get("sha256") or "") for row in formal_status_artifacts),
        },
    }


def _table_rows(tables: Mapping[str, Any], name: str) -> List[Dict[str, Any]]:
    rows = tables.get(name) or []
    return [dict(row) for row in rows if isinstance(row, Mapping)]


def _metadata(row: Mapping[str, Any]) -> Mapping[str, Any]:
    value = row.get("metadata")
    return value if isinstance(value, Mapping) else {}


def _raw_sample_header_evidence(artifacts: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    headers: set[str] = set()
    readable_paths: List[str] = []
    for row in artifacts:
        if row.get("artifact_role") != "raw_samples":
            continue
        path_text = str(row.get("path") or "").strip()
        if not path_text:
            continue
        path = Path(path_text)
        if not path.is_file():
            continue
        try:
            with path.open("r", encoding="utf-8-sig", newline="") as handle:
                header = next(csv.reader(handle), [])
        except Exception:
            continue
        readable_paths.append(str(path))
        headers.update(str(field).strip() for field in header if str(field).strip())
    return {
        "readable": bool(readable_paths),
        "path": readable_paths[0] if readable_paths else None,
        "paths": readable_paths,
        "headers": headers,
    }


def _component_name(row: Mapping[str, Any]) -> str:
    return str(row.get("component") or row.get("subject_id") or "").strip().lower()


def _candidate_device_id(row: Mapping[str, Any]) -> str:
    metadata = _metadata(row)
    for source in (row, metadata):
        for key in ("analyzer_device_id", "device_id", "analyzer_id", "serial_number"):
            value = str(source.get(key) or "").strip()
            if value:
                return value
    record_key = str(metadata.get("record_key") or "").strip()
    match = re.search(r"(\d{2,})$", record_key)
    if match:
        return match.group(1)
    return "unknown"


def _candidate_warning_reasons(row: Mapping[str, Any]) -> List[str]:
    metadata = _metadata(row)
    return _split_reasons(metadata.get("warning_reasons")) or _split_reasons(metadata.get("warnings")) or _split_reasons(
        metadata.get("warning_reasons_list")
    )


def _candidate_rollup_status(statuses: Sequence[str], blockers: Sequence[str], allowed_any: bool) -> str:
    lowered = [str(item or "").strip().lower() for item in statuses]
    if blockers or any("blocked" in item or "fail" in item for item in lowered):
        return "blocked"
    if any("review_required" in item or "requires" in item or "fit_review" in item for item in lowered):
        return "review_required"
    if allowed_any or any(item in {"ready_for_reviewer", "review_ready", "verification_passed"} for item in lowered):
        return "ready_for_reviewer"
    return lowered[0] if lowered else "not_evaluated"


def _candidate_rollup_action(status: str) -> str:
    if status == "blocked":
        return "resolve_blockers_before_coefficient_write_review"
    if status == "review_required":
        return "review_candidate_metrics_warnings_and_post_write_plan"
    if status == "ready_for_reviewer":
        return "reviewer_may_evaluate_candidate_no_auto_write"
    return "collect_or_attach_candidate_evidence"


def _candidate_review_rollup(coefficient_candidates: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[tuple[str, str], Dict[str, Any]] = {}
    for row in coefficient_candidates:
        component = _component_name(row) or "unknown"
        device_id = _candidate_device_id(row)
        key = (component, device_id)
        item = grouped.setdefault(
            key,
            {
                "component": component,
                "analyzer_device_id": device_id,
                "source_row_count": 0,
                "source_statuses": [],
                "allowed_for_review_any": False,
                "auto_write_allowed_any": False,
                "blockers": [],
                "warnings": [],
                "source_record_keys": [],
            },
        )
        item["source_row_count"] += 1
        status = str(row.get("candidate_status") or "").strip()
        if status and status not in item["source_statuses"]:
            item["source_statuses"].append(status)
        if _bool_value(row.get("allowed_for_review")):
            item["allowed_for_review_any"] = True
        if _bool_value(row.get("auto_write_allowed")):
            item["auto_write_allowed_any"] = True
        for reason in _split_reasons(row.get("blockers")):
            if reason and reason not in item["blockers"]:
                item["blockers"].append(reason)
        for reason in _candidate_warning_reasons(row):
            if reason and reason not in item["warnings"]:
                item["warnings"].append(reason)
        record_key = str(_metadata(row).get("record_key") or "").strip()
        if record_key and record_key not in item["source_record_keys"]:
            item["source_record_keys"].append(record_key)

    rows: List[Dict[str, Any]] = []
    for key, item in sorted(grouped.items(), key=lambda entry: entry[0]):
        consolidated_status = _candidate_rollup_status(
            item["source_statuses"],
            item["blockers"],
            bool(item["allowed_for_review_any"]),
        )
        rows.append(
            {
                **item,
                "consolidated_status": consolidated_status,
                "review_action": _candidate_rollup_action(consolidated_status),
                "auto_write_allowed_any": bool(item["auto_write_allowed_any"]),
                "allowed_for_review_any": bool(item["allowed_for_review_any"]),
            }
        )
    return rows


def build_traceability_summary_from_tables(
    tables: Mapping[str, Any],
    *,
    run_id: str = "",
    run_db_id: str = "",
    schema: str = "v1_5_evidence_registry",
    schema_version: str = "001",
) -> Dict[str, Any]:
    """Build a reviewer-friendly traceability summary from registry tables."""

    runs = _table_rows(tables, "runs")
    run = runs[0] if runs else {}
    resolved_run_id = str(run_id or run.get("run_id") or "")
    resolved_run_db_id = str(run_db_id or run.get("id") or "")
    artifacts = [
        {
            "artifact_role": row.get("artifact_role"),
            "path": row.get("path"),
            "sha256": row.get("sha256"),
            "required": _bool_value(row.get("required")),
            "size_bytes": row.get("size_bytes"),
        }
        for row in _table_rows(tables, "sample_files")
    ]
    required_artifacts = [row for row in artifacts if row["required"]]
    post_write_reverification_artifacts = [
        row
        for row in artifacts
        if str(row.get("artifact_role") or "") in POST_WRITE_REVERIFICATION_EVIDENCE_ROLES
    ]
    post_write_reverification_completion_artifacts = [
        row
        for row in post_write_reverification_artifacts
        if str(row.get("artifact_role") or "") in POST_WRITE_REVERIFICATION_COMPLETION_ROLES
    ]
    post_write_reverification_plan_artifacts = [
        row for row in artifacts if str(row.get("artifact_role") or "") == "post_run_reverification_plan"
    ]
    has_post_write_reverification = bool(post_write_reverification_completion_artifacts)
    run_evidence_status_artifacts = [
        row
        for row in artifacts
        if str(row.get("artifact_role") or "").startswith("run_evidence_status")
    ]
    formal_run_status_artifacts = [
        row
        for row in artifacts
        if str(row.get("artifact_role") or "").startswith("formal_run_status")
    ]
    missing_required_hashes = [
        str(row.get("artifact_role") or row.get("path") or "")
        for row in required_artifacts
        if not str(row.get("sha256") or "").strip()
    ]
    standard_gases = _table_rows(tables, "standard_gases")
    calibration_points = _table_rows(tables, "calibration_points")
    qc_results = _table_rows(tables, "qc_results")
    coefficient_candidates = _table_rows(tables, "coefficient_candidates")
    candidate_review_rollup = _candidate_review_rollup(coefficient_candidates)
    coefficient_snapshots = _table_rows(tables, "coefficient_snapshots")
    write_events = _table_rows(tables, "coefficient_write_events")
    write_attempts = [
        row for row in write_events if str(row.get("status") or "") not in NO_WRITE_EVENT_STATUSES
    ]
    write_policy = _coefficient_write_policy(
        coefficient_snapshots=coefficient_snapshots,
        write_events=write_events,
        has_post_write_reverification=has_post_write_reverification,
    )
    raw_header_evidence = _raw_sample_header_evidence(artifacts)
    raw_headers = raw_header_evidence["headers"]
    h2o_raw_evidence = _h2o_raw_field_evidence(raw_headers)
    missing_h2o_raw_fields = h2o_raw_evidence["missing"]
    h2o_gases = [row for row in standard_gases if _component_name(row) == "h2o"]
    h2o_points = [row for row in calibration_points if _component_name(row) == "h2o"]
    h2o_qc_rows = [
        row
        for row in qc_results
        if str(row.get("scope") or "").strip().lower() == "component"
        and str(row.get("subject_id") or "").strip().lower() == "h2o"
    ]
    h2o_candidates = [row for row in coefficient_candidates if _component_name(row) == "h2o"]
    h2o_candidate_rollup = [row for row in candidate_review_rollup if str(row.get("component") or "") == "h2o"]
    h2o_a_grade_count = sum(int(row.get("a_grade_count") or 0) for row in h2o_points)
    h2o_rejected_count = sum(int(row.get("rejected_count") or 0) for row in h2o_points)
    h2o_open_flow_points = [
        row for row in h2o_points if str(row.get("pressure_mode") or "").strip().lower() == "ambient_open"
    ]
    h2o_open_flow_qc = [
        row
        for row in h2o_qc_rows
        if _metadata(row).get("physical_scope") == "open_flow_component_stability"
    ]
    water_route_evidence = {
        "physical_scope": "open_flow_h2o_water_route_evidence",
        "h2o_standard_reference_present": bool(h2o_gases),
        "h2o_calibration_points_present": bool(h2o_points),
        "h2o_open_flow_points_present": bool(h2o_open_flow_points),
        "h2o_a_grade_count": h2o_a_grade_count,
        "h2o_rejected_count": h2o_rejected_count,
        "h2o_qc_present": bool(h2o_qc_rows),
        "h2o_open_flow_qc_present": bool(h2o_open_flow_qc),
        "h2o_candidate_review_present": bool(h2o_candidates),
        "h2o_candidate_device_count": len(h2o_candidate_rollup),
        "h2o_candidate_rollup_statuses": sorted(
            {str(row.get("consolidated_status") or "") for row in h2o_candidate_rollup if row.get("consolidated_status")}
        ),
        "raw_sample_header_readable": bool(raw_header_evidence["readable"]),
        "raw_sample_header_path": raw_header_evidence["path"],
        "raw_sample_header_paths": raw_header_evidence.get("paths") or [],
        "required_raw_h2o_fields": h2o_raw_evidence["required"],
        "present_raw_h2o_fields": h2o_raw_evidence["present"],
        "missing_raw_h2o_fields": missing_h2o_raw_fields,
        "optional_missing_raw_h2o_fields": h2o_raw_evidence["optional_missing_base"],
        "raw_h2o_fields_present": bool(h2o_raw_evidence["complete"]),
        "h2o_base_fields_by_group": h2o_raw_evidence["base_fields_by_group"],
        "h2o_analyzer_fields_by_suffix": h2o_raw_evidence["analyzer_fields_by_suffix"],
        "interpretation": (
            "H2O open-flow evidence must preserve the humidity reference, dewpoint, and analyzer H2O "
            "ratio/raw-ratio/ref/signal fields so water-route stability can be reviewed separately from CO2 "
            "fitting. Derived dry/wet ppmv fields are useful normalization evidence, but are optional when "
            "dewpoint and reference H2O amount are available for offline recomputation."
        ),
    }
    return {
        "schema": schema,
        "schema_version": schema_version,
        "run_id": resolved_run_id,
        "run_db_id": resolved_run_db_id,
        "evidence_status": run.get("evidence_status"),
        "package_status": run.get("package_status"),
        "physical_boundaries": {
            "sidecar_only": True,
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "controls_valves_or_pace": False,
            "writes_coefficients": bool(write_attempts),
            "not_real_acceptance_evidence": True,
        },
        "table_counts": {name: len(_table_rows(tables, name)) for name in TABLE_NAMES},
        "artifact_count": len(artifacts),
        "required_artifact_count": len(required_artifacts),
        "missing_required_artifact_hashes": missing_required_hashes,
        "artifacts": artifacts,
        "devices": [
            {
                "device_type": row.get("device_type"),
                "device_role": row.get("device_role"),
                "display_name": row.get("display_name"),
                "serial_number": row.get("serial_number"),
            }
            for row in _table_rows(tables, "devices")
        ],
        "standard_gases": [
            {
                "component": row.get("component"),
                "cylinder_id": row.get("cylinder_id"),
                "certificate_value": row.get("certificate_value"),
                "certificate_uncertainty": row.get("certificate_uncertainty"),
                "valid_until": row.get("valid_until"),
                "supplier": row.get("supplier"),
                "certificate_hash": row.get("certificate_hash"),
            }
            for row in standard_gases
        ],
        "reference_certificates": [
            {
                "reference_role": row.get("reference_role"),
                "certificate_id": row.get("certificate_id"),
                "certificate_hash": row.get("certificate_hash"),
                "valid_until": row.get("valid_until"),
                "uncertainty": row.get("uncertainty"),
                "unit": row.get("unit"),
            }
            for row in _table_rows(tables, "reference_certificates")
        ],
        "calibration_points": [
            {
                "component": row.get("component"),
                "point_key": row.get("point_key"),
                "pressure_mode": row.get("pressure_mode"),
                "sample_count": row.get("sample_count"),
                "a_grade_count": row.get("a_grade_count"),
                "b_grade_count": row.get("b_grade_count"),
                "rejected_count": row.get("rejected_count"),
            }
            for row in calibration_points
        ],
        "qc_results": [
            {
                "scope": row.get("scope"),
                "subject_id": row.get("subject_id"),
                "rule_name": row.get("rule_name"),
                "status": row.get("status"),
                "severity": row.get("severity"),
                "reasons": row.get("reasons"),
                "physical_scope": _metadata(row).get("physical_scope"),
            }
            for row in qc_results
        ],
        "coefficient_candidates": [
            {
                "component": row.get("component"),
                "analyzer_device_id": _candidate_device_id(row),
                "candidate_status": row.get("candidate_status"),
                "allowed_for_review": row.get("allowed_for_review"),
                "auto_write_allowed": row.get("auto_write_allowed"),
                "blockers": row.get("blockers"),
                "warnings": _candidate_warning_reasons(row),
            }
            for row in coefficient_candidates
        ],
        "candidate_review_rollup": candidate_review_rollup,
        "h2o_candidate_review_rollup": h2o_candidate_rollup,
        "coefficient_write_evidence": write_policy,
        "coefficient_write_events": [
            {
                "event_type": row.get("event_type"),
                "status": row.get("status"),
                "approved_by": row.get("approved_by"),
                "command_summary": row.get("command_summary"),
            }
            for row in write_events
        ],
        "reports": [
            {
                "report_type": row.get("report_type"),
                "path": row.get("path"),
                "sha256": row.get("sha256"),
                "status": row.get("status"),
            }
            for row in _table_rows(tables, "reports")
        ],
        "run_evidence_status_artifacts": [
            {
                "artifact_role": row.get("artifact_role"),
                "path": row.get("path"),
                "sha256": row.get("sha256"),
                "required": row.get("required"),
            }
            for row in run_evidence_status_artifacts
        ],
        "formal_run_status_artifacts": [
            {
                "artifact_role": row.get("artifact_role"),
                "path": row.get("path"),
                "sha256": row.get("sha256"),
                "required": row.get("required"),
            }
            for row in formal_run_status_artifacts
        ],
        "post_write_reverification_evidence": {
            "status": (
                "present"
                if has_post_write_reverification
                else ("planned_only" if post_write_reverification_plan_artifacts else "missing")
            ),
            "completion_roles_present": sorted(
                {str(row.get("artifact_role") or "") for row in post_write_reverification_completion_artifacts}
            ),
            "support_roles_present": sorted(
                {str(row.get("artifact_role") or "") for row in post_write_reverification_artifacts}
            ),
            "artifact_paths": [
                str(row.get("path") or "") for row in post_write_reverification_artifacts[:20]
            ],
            "plan_artifact_paths": [
                str(row.get("path") or "") for row in post_write_reverification_plan_artifacts[:20]
            ],
            "interpretation": (
                "Post-write reverification is complete only when actual post-write point or device-summary "
                "evidence is present. A post_write_reverification_plan.csv is a required plan for the next action, "
                "but it is not itself proof that the written coefficients were reverified."
            ),
        },
        "integrity_checks": [
            {
                "check_name": row.get("check_name"),
                "status": row.get("status"),
                "severity": row.get("severity"),
                "details": row.get("details"),
            }
            for row in _table_rows(tables, "evidence_integrity_checks")
        ],
        "water_route_evidence": water_route_evidence,
        "traceability_checks": {
            "all_required_artifacts_have_sha256": not missing_required_hashes,
            "no_coefficient_write_attempted": not write_attempts,
            "has_standard_gas_traceability": bool(standard_gases),
            "has_pressure_reference_traceability": bool(_table_rows(tables, "reference_certificates")),
            "has_raw_samples": any(row.get("artifact_role") == "raw_samples" for row in artifacts),
            "has_pressure_quick_check": any(
                row.get("artifact_role") in {"pressure_channel_quick_check", "pressure_channel_completion"}
                for row in artifacts
            ),
            "has_water_route_traceability": bool(h2o_gases and h2o_open_flow_points),
            "has_h2o_open_flow_qc": bool(h2o_open_flow_qc),
            "has_h2o_raw_signal_fields": not missing_h2o_raw_fields,
            "has_post_write_reverification": has_post_write_reverification,
            "has_controlled_write_traceability": write_policy["status"] == "controlled_write_traceable",
            "coefficient_write_policy_satisfied": bool(write_policy["policy_satisfied"]),
            "has_run_evidence_status": bool(run_evidence_status_artifacts),
            "has_formal_run_status": bool(formal_run_status_artifacts),
        },
    }


def bundle_traceability_summary(bundle: Mapping[str, Any]) -> Dict[str, Any]:
    """Return the traceability view for an evidence bundle."""

    tables = bundle.get("tables") if isinstance(bundle.get("tables"), Mapping) else {}
    return build_traceability_summary_from_tables(
        tables,
        run_id=str(bundle.get("run_id") or ""),
        run_db_id=str(bundle.get("run_db_id") or ""),
        schema=str(bundle.get("schema") or "v1_5_evidence_registry"),
        schema_version=str(bundle.get("schema_version") or "001"),
    )


def verify_evidence_bundle_integrity(bundle: Mapping[str, Any]) -> Dict[str, Any]:
    """Verify that an evidence bundle still matches its on-disk artifacts.

    This is an offline audit helper. It only reads sidecar files and does not
    open COM ports, control routes, or write calibration coefficients.
    """

    tables = bundle.get("tables") if isinstance(bundle.get("tables"), Mapping) else {}
    if not isinstance(tables, Mapping):
        tables = {}
    sample_files = list(tables.get("sample_files") or [])
    reports = list(tables.get("reports") or [])
    checks: List[Dict[str, Any]] = []

    missing_files = []
    mismatched_files = []
    unhashed_files = []
    required_missing = []
    for row in sample_files:
        artifact_id = str(row.get("id") or "")
        role = str(row.get("artifact_role") or "")
        path_text = str(row.get("path") or "")
        expected_hash = str(row.get("sha256") or "")
        if not expected_hash:
            unhashed_files.append({"id": artifact_id, "artifact_role": role, "path": path_text})
            continue
        path = Path(path_text)
        if not path.exists():
            missing_files.append({"id": artifact_id, "artifact_role": role, "path": path_text})
            if row.get("required"):
                required_missing.append({"id": artifact_id, "artifact_role": role, "path": path_text})
            continue
        actual_hash = sha256_file(path)
        if actual_hash != expected_hash:
            mismatched_files.append(
                {
                    "id": artifact_id,
                    "artifact_role": role,
                    "path": path_text,
                    "expected_sha256": expected_hash,
                    "actual_sha256": actual_hash,
                }
            )

    checks.append(
        {
            "check_name": "sample_file_hashes_match_disk",
            "status": "pass" if not missing_files and not mismatched_files and not unhashed_files else "fail",
            "severity": "error",
            "details": {
                "sample_file_count": len(sample_files),
                "missing_files": missing_files,
                "mismatched_files": mismatched_files,
                "unhashed_files": unhashed_files,
            },
        }
    )
    checks.append(
        {
            "check_name": "required_artifacts_present_on_disk",
            "status": "pass" if not required_missing else "fail",
            "severity": "error",
            "details": {"missing_required_artifacts": required_missing},
        }
    )

    artifact_by_id = {str(row.get("id") or ""): row for row in sample_files}
    report_link_failures = []
    for row in reports:
        metadata = row.get("metadata") if isinstance(row.get("metadata"), Mapping) else {}
        source_id = str(metadata.get("source_artifact_id") or "")
        source = artifact_by_id.get(source_id)
        if not source:
            report_link_failures.append(
                {
                    "report_type": row.get("report_type"),
                    "path": row.get("path"),
                    "source_artifact_id": source_id,
                    "reason": "missing_source_artifact",
                }
            )
            continue
        if str(source.get("sha256") or "") != str(row.get("sha256") or ""):
            report_link_failures.append(
                {
                    "report_type": row.get("report_type"),
                    "path": row.get("path"),
                    "source_artifact_id": source_id,
                    "reason": "sha256_mismatch_with_source_artifact",
                }
            )
        if str(source.get("path") or "") != str(row.get("path") or ""):
            report_link_failures.append(
                {
                    "report_type": row.get("report_type"),
                    "path": row.get("path"),
                    "source_artifact_id": source_id,
                    "reason": "path_mismatch_with_source_artifact",
                }
            )
    checks.append(
        {
            "check_name": "report_rows_link_to_sample_files",
            "status": "pass" if not report_link_failures else "fail",
            "severity": "error",
            "details": {
                "report_count": len(reports),
                "link_failures": report_link_failures,
            },
        }
    )

    failed = [row for row in checks if row.get("status") != "pass"]
    return {
        "schema": "v1_5_evidence_bundle_integrity_check_v1",
        "run_id": str(bundle.get("run_id") or ""),
        "run_db_id": str(bundle.get("run_db_id") or ""),
        "status": "pass" if not failed else "fail",
        "check_count": len(checks),
        "failed_check_count": len(failed),
        "checks": checks,
        "physical_boundaries": {
            "sidecar_only": True,
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "controls_valves_or_pace": False,
            "writes_coefficients": False,
            "not_real_acceptance_evidence": True,
        },
    }


def write_bundle_json(bundle: Mapping[str, Any], path: str | Path) -> Path:
    target = Path(path).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(bundle, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return target
