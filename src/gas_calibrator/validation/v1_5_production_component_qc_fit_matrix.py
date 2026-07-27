"""Build production-semantics component QC and the canonical 0613 fit matrix.

The evaluator reads immutable point artifacts into a central review bundle.  It
never backfills historical point directories and never executes a coefficient
fit when route continuity is not formally attested.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import re
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .v1_5_component_qc_generator_contract import (
    FORMAL_COMPONENT_QC_REQUIRED_ARTIFACTS,
    FORMAL_COMPONENT_QC_REQUIRED_ARTIFACTS_V1,
    FORMAL_EVIDENCE_BUNDLE_FILENAME,
    FORMAL_EVIDENCE_BUNDLE_SCHEMA,
    FORMAL_EVIDENCE_BUNDLE_SCHEMA_V1,
    FORMAL_REFERENCE_SOURCE_RECORD_FILENAME,
    FORMAL_REFERENCE_SOURCE_RECORD_SCHEMA,
    evaluate_component_qc_temporal_window,
    validate_v1_5_component_qc_generator_contract,
)


SCHEMA = "v1_5_production_component_qc_fit_matrix_v1"
EVIDENCE_SOURCE = "historical_replay"
OUTPUT_SUFFIX = (
    "docs",
    "v1_5_flow_contract",
    "production_component_qc_fit_matrix",
)

GRADE_A = "A_calibration_eligible"
GRADE_B = "B_diagnostic_model_only"
GRADE_C = "C_reject"
GRADE_RANK = {GRADE_A: 0, GRADE_B: 1, GRADE_C: 2}

MODEL_FAMILIES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("linear_R", ("intercept", "R")),
    ("quadratic_R", ("intercept", "R", "R2")),
    ("cubic_R", ("intercept", "R", "R2", "R3")),
    ("quadratic_R_T_RT", ("intercept", "R", "R2", "T", "RT")),
    ("cubic_R_T", ("intercept", "R", "R2", "R3", "T")),
    ("cubic_R_T_RT", ("intercept", "R", "R2", "R3", "T", "RT")),
    ("cubic_R_T_T2_RT", ("intercept", "R", "R2", "R3", "T", "T2", "RT")),
)

POINT_SELECTION_STRATEGIES: tuple[tuple[str, str], ...] = (
    ("canonical_all_a_continuous", "production_candidate"),
    ("latest_same_state_supersede", "production_candidate"),
    ("leave_one_temperature_out", "diagnostic_only"),
    ("single_point_exclusion_sensitivity", "diagnostic_only"),
    ("anchor_sensitivity", "diagnostic_only"),
)

_PREFIX_RE = re.compile(r"^(ga\d{2})_(co2|h2o)_ratio_f$", re.IGNORECASE)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return payload


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _payload_sha256(payload: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "pass", "ready"}


def _nested(payload: Mapping[str, Any], path: Sequence[str], default: Any = None) -> Any:
    current: Any = payload
    for key in path:
        if not isinstance(current, Mapping):
            return default
        current = current.get(key)
    return default if current is None else current


def _worse(left: str, right: str) -> str:
    return left if GRADE_RANK[left] >= GRADE_RANK[right] else right


def _artifact_groups(preflight: Mapping[str, Any]) -> dict[str, dict[str, Mapping[str, Any]]]:
    grouped: dict[str, dict[str, Mapping[str, Any]]] = defaultdict(dict)
    for row in preflight.get("artifact_checks") or []:
        if not isinstance(row, Mapping):
            continue
        point_dir = str(row.get("point_dir") or "").strip()
        role = str(row.get("role") or "").strip()
        if point_dir and role:
            grouped[str(Path(point_dir).resolve())][role] = row
    return grouped


def _bound_artifact(
    checks: Mapping[str, Mapping[str, Any]], role: str, reasons: list[str]
) -> Path | None:
    row = checks.get(role)
    if not isinstance(row, Mapping):
        reasons.append(f"source_artifact_missing:{role}")
        return None
    path = Path(str(row.get("path") or ""))
    if row.get("status") != "pass" or not path.is_file():
        reasons.append(f"source_artifact_not_pass:{role}")
        return None
    actual = _sha256(path)
    if actual != str(row.get("recorded_sha256") or ""):
        reasons.append(f"source_artifact_sha_mismatch:{role}")
        return None
    return path


def _route_target(component: str, rows: Sequence[Mapping[str, Any]], sidecar: Mapping[str, Any]) -> float | None:
    sample_key = "co2_ppm_target" if component == "co2" else "h2o_mmol_target"
    sample_values = {_finite(row.get(sample_key)) for row in rows}
    sample_values.discard(None)
    if len(sample_values) == 1:
        return next(iter(sample_values))
    sidecar_key = "co2_source_ppm" if component == "co2" else "certificate_h2o_mmol"
    return _finite(sidecar.get(sidecar_key))


def _analyzer_prefixes(rows: Sequence[Mapping[str, Any]], component: str) -> list[str]:
    if not rows:
        return []
    prefixes = {
        match.group(1).lower()
        for key in rows[0]
        if (match := _PREFIX_RE.match(key)) and match.group(2).lower() == component
    }
    return sorted(prefixes)


def _required_count(runtime: Mapping[str, Any], component: str, contract: Mapping[str, Any]) -> int:
    value = _nested(
        runtime,
        ("workflow", "stability", "sensor", f"{component}_ratio_f_preseal_min_samples"),
    )
    try:
        count = int(value)
    except (TypeError, ValueError):
        count = int((contract.get("scope") or {}).get("default_required_sample_count") or 10)
    return max(1, count)


def _expected_sample_interval_s(
    runtime: Mapping[str, Any], component: str
) -> float | None:
    sampling = _nested(runtime, ("workflow", "sampling"), {})
    if not isinstance(sampling, Mapping):
        return None
    for key in (f"{component}_interval_s", "interval_s"):
        value = _finite(sampling.get(key))
        if value is not None and value > 0:
            return value
    return None


def _run_id(value: Any) -> str:
    return str(value or "").strip()


def _read_optional_json(path: Path) -> tuple[dict[str, Any], str | None]:
    if not path.is_file():
        return {}, None
    try:
        return _read_json(path), None
    except (OSError, ValueError, json.JSONDecodeError):
        return {}, "json_invalid"


def _verify_formal_evidence_bundle(
    point_dir: Path,
    *,
    component: str,
    expected_run_id: str,
) -> dict[str, Any]:
    manifest_path = point_dir / FORMAL_EVIDENCE_BUNDLE_FILENAME
    manifest, read_error = _read_optional_json(manifest_path)
    reasons: list[str] = []
    manifest_schema = manifest.get("schema_version")
    required_by_schema = {
        FORMAL_EVIDENCE_BUNDLE_SCHEMA: FORMAL_COMPONENT_QC_REQUIRED_ARTIFACTS,
        FORMAL_EVIDENCE_BUNDLE_SCHEMA_V1: FORMAL_COMPONENT_QC_REQUIRED_ARTIFACTS_V1,
    }
    required = required_by_schema.get(
        manifest_schema,
        FORMAL_COMPONENT_QC_REQUIRED_ARTIFACTS,
    )[component]
    if read_error:
        reasons.append("bundle_manifest_json_invalid")
    if manifest_schema not in required_by_schema:
        reasons.append("bundle_manifest_schema_mismatch")
    if _run_id(manifest.get("run_id")) != expected_run_id:
        reasons.append("bundle_manifest_run_id_mismatch")
    if str(manifest.get("route_kind") or "").lower() != component:
        reasons.append("bundle_manifest_route_kind_mismatch")
    if manifest.get("identity_contract") != "immutable_claim_runtime_run_id_and_sha256_bundle":
        reasons.append("bundle_manifest_identity_contract_mismatch")
    if manifest.get("bundle_complete") is not True:
        reasons.append("bundle_manifest_not_complete")
    if list(manifest.get("missing_required_roles") or []):
        reasons.append("bundle_manifest_missing_required_roles")
    if sorted(manifest.get("required_roles") or []) != sorted(required):
        reasons.append("bundle_manifest_required_roles_mismatch")

    artifact_rows = manifest.get("artifacts")
    artifact_index: dict[str, Mapping[str, Any]] = {}
    if not isinstance(artifact_rows, list):
        reasons.append("bundle_manifest_artifacts_not_list")
        artifact_rows = []
    for row in artifact_rows:
        if not isinstance(row, Mapping):
            reasons.append("bundle_manifest_artifact_row_invalid")
            continue
        role = str(row.get("role") or "")
        if not role or role in artifact_index:
            reasons.append("bundle_manifest_artifact_role_invalid_or_duplicate")
            continue
        artifact_index[role] = row
    if sorted(artifact_index) != sorted(required):
        reasons.append("bundle_manifest_artifact_roles_mismatch")
    for role, filename in required.items():
        row = artifact_index.get(role)
        if not row:
            continue
        if str(row.get("filename") or "") != filename:
            reasons.append(f"bundle_manifest_filename_mismatch:{role}")
            continue
        path = point_dir / filename
        if not path.is_file():
            reasons.append(f"bundle_manifest_source_missing:{role}")
            continue
        if str(row.get("sha256") or "") != _sha256(path):
            reasons.append(f"bundle_manifest_sha_mismatch:{role}")
        try:
            recorded_size = int(row.get("size_bytes"))
        except (TypeError, ValueError):
            recorded_size = -1
        if recorded_size != path.stat().st_size:
            reasons.append(f"bundle_manifest_size_mismatch:{role}")

    if manifest_schema == FORMAL_EVIDENCE_BUNDLE_SCHEMA:
        operator_record, operator_error = _read_optional_json(
            point_dir / required["operator_confirmation"]
        )
        if operator_error:
            reasons.append("operator_confirmation_record_invalid")
        else:
            if operator_record.get("engineering_probe_only") is not True:
                reasons.append("operator_confirmation_engineering_probe_only_missing")
            if operator_record.get("operator_confirmation_matches_required") is not True:
                reasons.append("operator_confirmation_mismatch")
            if operator_record.get("no_write") is not True:
                reasons.append("operator_confirmation_no_write_false")
            if operator_record.get("ftd_write_enabled") is not False:
                reasons.append("operator_confirmation_ftd_write_not_false")
            if operator_record.get("promotion_state") != "blocked":
                reasons.append("operator_confirmation_promotion_state_not_blocked")
            if operator_record.get("not_real_acceptance_evidence") is not True:
                reasons.append("operator_confirmation_real_acceptance_flag_invalid")

        shutdown, shutdown_error = _read_optional_json(
            point_dir / required["physical_shutdown"]
        )
        if shutdown_error:
            reasons.append("physical_shutdown_status_invalid")
        else:
            if str(shutdown.get("route_kind") or "").lower() != component:
                reasons.append("physical_shutdown_route_kind_mismatch")
            if shutdown.get("overall_status") != "pass":
                reasons.append("physical_shutdown_not_pass")
            if list(shutdown.get("critical_failures") or []):
                reasons.append("physical_shutdown_critical_failures_present")

        temperature_trace_path = point_dir / required["temperature_truth_trace"]
        temperature_rows: list[Mapping[str, Any]] = []
        try:
            for line in temperature_trace_path.read_text(encoding="utf-8-sig").splitlines():
                if not line.strip():
                    continue
                payload = json.loads(line)
                if not isinstance(payload, Mapping):
                    raise ValueError("temperature_trace_row_not_object")
                temperature_rows.append(payload)
        except (OSError, ValueError, json.JSONDecodeError):
            reasons.append("temperature_truth_trace_invalid")
        required_temperature_stages = {"pre_route", "pre_sample", "post_sample"}
        observed_temperature_stages = {
            str(row.get("stage") or "") for row in temperature_rows
        }
        if not required_temperature_stages.issubset(observed_temperature_stages):
            reasons.append("temperature_truth_required_stages_missing")
        if any(
            row.get("ok") is not True
            for row in temperature_rows
            if str(row.get("stage") or "") in required_temperature_stages
        ):
            reasons.append("temperature_truth_stage_not_pass")

    canonical = {
        key: manifest.get(key)
        for key in (
            "schema_version",
            "run_id",
            "route_kind",
            "identity_contract",
            "required_roles",
            "artifacts",
            "missing_required_roles",
            "bundle_complete",
        )
    }
    computed_bundle_sha256 = _payload_sha256(canonical)
    recorded_bundle_sha256 = str(manifest.get("bundle_sha256") or "")
    if computed_bundle_sha256 != recorded_bundle_sha256:
        reasons.append("bundle_manifest_digest_mismatch")
    return {
        "verified": not reasons,
        "reason_codes": sorted(set(reasons)),
        "bundle_sha256": recorded_bundle_sha256,
        "member_count": len(artifact_index),
    }


def _evaluate_evidence_identity(
    *,
    point_dir: Path,
    component: str,
    sample_rows: Sequence[Mapping[str, Any]],
    runtime: Mapping[str, Any],
    sidecar: Mapping[str, Any],
    route_timing: Mapping[str, Any],
    bound_paths: Mapping[str, Path],
) -> dict[str, Any]:
    expected_run_id = point_dir.name
    reasons: list[str] = []
    sample_run_ids = sorted(
        {
            _run_id(row.get("run_id"))
            for row in sample_rows
            if _run_id(row.get("run_id"))
        }
    )
    if len(sample_run_ids) != 1:
        reasons.append("sample_run_id_missing_or_changes")
    elif sample_run_ids[0] != expected_run_id:
        reasons.append("sample_run_id_mismatch")
    sidecar_run_id = _run_id(sidecar.get("run_id"))
    if not sidecar_run_id:
        reasons.append("sidecar_run_id_missing")
    elif sidecar_run_id != expected_run_id:
        reasons.append("sidecar_run_id_mismatch")
    route_timing_run_id = _run_id(route_timing.get("run_id"))
    if component == "co2":
        if not route_timing_run_id:
            reasons.append("route_timing_run_id_missing")
        elif route_timing_run_id != expected_run_id:
            reasons.append("route_timing_run_id_mismatch")

    runtime_run_id = _run_id((runtime.get("metadata") or {}).get("run_id"))
    claim_path = point_dir / "run_directory_claim.json"
    manifest_path = point_dir / FORMAL_EVIDENCE_BUNDLE_FILENAME
    claim, claim_error = _read_optional_json(claim_path)
    claim_run_id = _run_id(claim.get("run_id"))
    strict_marker_present = (
        claim_path.is_file() or manifest_path.is_file() or bool(runtime_run_id)
    )
    bundle_verified = False
    if strict_marker_present:
        mode = "strict_claim_runtime_bundle"
        if claim_error:
            reasons.append("run_directory_claim_json_invalid")
        if not claim_path.is_file():
            reasons.append("run_directory_claim_missing")
        elif claim.get("schema_version") != "immutable_run_directory_claim_v1":
            reasons.append("run_directory_claim_schema_mismatch")
        if not claim_run_id:
            reasons.append("run_directory_claim_run_id_missing")
        elif claim_run_id != expected_run_id:
            reasons.append("run_directory_claim_run_id_mismatch")
        if not runtime_run_id:
            reasons.append("runtime_config_run_id_missing")
        elif runtime_run_id != expected_run_id:
            reasons.append("runtime_config_run_id_mismatch")
        if not manifest_path.is_file():
            reasons.append("formal_evidence_bundle_manifest_missing")
            bundle = {
                "verified": False,
                "reason_codes": [],
                "bundle_sha256": "",
                "member_count": 0,
            }
        else:
            bundle = _verify_formal_evidence_bundle(
                point_dir,
                component=component,
                expected_run_id=expected_run_id,
            )
            bundle_verified = bundle["verified"]
            reasons.extend(bundle["reason_codes"])
    else:
        mode = "legacy_run_id_consensus"
        members = [
            {
                "role": role,
                "filename": path.name,
                "sha256": _sha256(path),
                "size_bytes": path.stat().st_size,
            }
            for role, path in sorted(bound_paths.items())
            if path.is_file()
        ]
        bundle = {
            "verified": False,
            "reason_codes": [],
            "bundle_sha256": _payload_sha256(
                {
                    "schema_version": "v1_5_legacy_review_evidence_bundle_v1",
                    "run_id": expected_run_id,
                    "route_kind": component,
                    "artifacts": members,
                }
            ),
            "member_count": len(members),
        }
    return {
        "evidence_run_id": expected_run_id,
        "evidence_identity_status": "pass" if not reasons else "fail",
        "evidence_identity_mode": mode,
        "evidence_identity_reason_codes": sorted(set(reasons)),
        "evidence_sample_run_ids": sample_run_ids,
        "evidence_sidecar_run_id": sidecar_run_id,
        "evidence_route_timing_run_id": route_timing_run_id,
        "evidence_runtime_run_id": runtime_run_id,
        "evidence_claim_run_id": claim_run_id,
        "evidence_bundle_sha256": bundle["bundle_sha256"],
        "evidence_bundle_member_count": bundle["member_count"],
        "evidence_bundle_manifest_verified": bundle_verified,
        "not_real_acceptance_evidence": True,
    }


def _evaluate_reference_source_record(
    point_dir: Path,
    *,
    component: str,
    expected_run_id: str,
    strict_identity: bool,
) -> dict[str, Any]:
    path = point_dir / FORMAL_REFERENCE_SOURCE_RECORD_FILENAME
    if not path.is_file():
        return {
            "reference_source_status": (
                "fail" if strict_identity else "legacy_not_recorded"
            ),
            "reference_source_record_present": False,
            "reference_source_record_valid": False,
            "reference_source_reason_codes": (
                ["reference_source_record_missing"] if strict_identity else []
            ),
            "reference_asset_id": "",
            "reference_value_source": "",
            "reference_source_sha256": "",
        }
    record, read_error = _read_optional_json(path)
    reasons: list[str] = []
    if read_error:
        reasons.append("reference_source_record_json_invalid")
    if record.get("schema_version") != FORMAL_REFERENCE_SOURCE_RECORD_SCHEMA:
        reasons.append("reference_source_record_schema_invalid")
    if _run_id(record.get("run_id")) != expected_run_id:
        reasons.append("reference_source_record_run_id_mismatch")
    if str(record.get("route_kind") or "").lower() != component:
        reasons.append("reference_source_record_route_kind_mismatch")
    if record.get("reference_source_status") != "pass":
        reasons.append("reference_source_gate_not_passed")
    if record.get("not_real_acceptance_evidence") is not True:
        reasons.append("reference_source_real_acceptance_lock_missing")
    if component == "co2":
        asset = record.get("selected_asset")
        if not isinstance(asset, Mapping):
            reasons.append("co2_reference_asset_missing")
            asset = {}
        documents = record.get("documents_verified")
        if not isinstance(documents, list) or not documents:
            reasons.append("co2_reference_documents_missing")
        elif any(not isinstance(row, Mapping) or row.get("verified") is not True for row in documents):
            reasons.append("co2_reference_document_not_verified")
        nominal = _finite(asset.get("nominal_co2_ppm"))
        if nominal == 0.0:
            if asset.get("reference_value_source") != "operator_confirmed_previous_calibration":
                reasons.append("co2_zero_reference_value_source_invalid")
            if asset.get("co2_value_directly_certified") is not False:
                reasons.append("co2_zero_direct_certificate_claim_invalid")
        elif asset.get("co2_value_directly_certified") is not True:
            reasons.append("co2_standard_direct_certificate_missing")
        asset_id = str(asset.get("asset_id") or "")
        value_source = str(asset.get("reference_value_source") or "")
    else:
        primary = record.get("h2o_concentration_reference")
        flow = record.get("humidity_generator_flow")
        route_flow = record.get("route_flow_evidence")
        if record.get("reference_value_source") != "measured_dewpoint_plus_measured_pressure":
            reasons.append("h2o_reference_value_source_invalid")
        if not isinstance(primary, Mapping):
            reasons.append("h2o_concentration_reference_missing")
            primary = {}
        quantities = set(primary.get("primary_quantities") or [])
        if quantities != {
            "actual_dewpoint_meter_measurement",
            "actual_pressure_measurement_bound_in_samples",
        }:
            reasons.append("h2o_dewpoint_pressure_reference_incomplete")
        if not isinstance(flow, Mapping) or flow.get("role") != "source_state_evidence_only":
            reasons.append("h2o_flow_role_invalid")
        if (
            not isinstance(route_flow, Mapping)
            or route_flow.get("role") != "route_and_process_evidence_only"
            or route_flow.get("source")
            not in {"dewpoint_meter_output", "humidity_generator_state_fallback"}
            or _finite(route_flow.get("observed_flow_lpm")) is None
        ):
            reasons.append("h2o_route_flow_evidence_invalid")
        asset_id = str(record.get("reference_asset_id") or "")
        value_source = str(record.get("reference_value_source") or "")
    return {
        "reference_source_status": "pass" if not reasons else "fail",
        "reference_source_record_present": True,
        "reference_source_record_valid": not reasons,
        "reference_source_reason_codes": sorted(set(reasons)),
        "reference_asset_id": asset_id,
        "reference_value_source": value_source,
        "reference_source_sha256": _sha256(path),
    }


def _point_hard_blockers(
    *,
    component: str,
    candidate: Mapping[str, Any],
    sample_rows: Sequence[Mapping[str, Any]],
    sidecar: Mapping[str, Any],
    route_timing: Mapping[str, Any],
    humidity_reference: Mapping[str, Any],
) -> list[str]:
    blockers: list[str] = []
    if not sample_rows:
        blockers.append("sample_window_missing")
    route_open_until_end = sidecar.get("route_open_until_sample_end") is True
    if component == "co2":
        route_open_until_end = route_open_until_end and route_timing.get("route_opened") is True
        if route_timing.get("sampling_before_route_close") is not True:
            blockers.append("sampling_after_route_close")
    if not route_open_until_end:
        blockers.append("route_not_open_until_sample_end")
    if any(_truthy(row.get("point_quality_blocked")) for row in sample_rows):
        blockers.append("point_quality_blocked")
    target = _route_target(component, sample_rows, sidecar)
    if target is None or target < 0:
        blockers.append("reference_target_missing_or_invalid")
    if component == "h2o":
        review = humidity_reference.get("humidity_reference_check") or {}
        if not isinstance(review, Mapping) or review.get("hard_block") is True:
            blockers.append("route_specific_physical_reference_invalid")
        if candidate.get("purge_below_declared_minimum") is True:
            blockers.append("route_specific_physical_reference_invalid")
    return sorted(set(blockers))


def _evaluate_analyzer(
    *,
    component: str,
    prefix: str,
    rows: Sequence[Mapping[str, Any]],
    required_count: int,
    hard_blockers: Sequence[str],
    temporal_evidence: Mapping[str, Any],
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    route = (contract.get("routes") or {})[component]
    ratio_key = f"{prefix}_{component}_ratio_f"
    usable_key = f"{prefix}_frame_usable"
    ids = {
        str(row.get(f"{prefix}_analyzer_device_id") or "").strip()
        for row in rows
        if str(row.get(f"{prefix}_analyzer_device_id") or "").strip()
    }
    reasons: list[str] = []
    grade = GRADE_A
    if len(ids) != 1:
        grade = GRADE_C
        reasons.append("analyzer_identity_missing_or_changes_in_window")
    if temporal_evidence.get("temporal_window_complete") is not True:
        grade = GRADE_C
        reasons.append("missing_timestamps_or_incomplete_window")
        reasons.extend(
            f"temporal:{value}"
            for value in temporal_evidence.get("temporal_reason_codes") or []
        )

    ratios: list[float] = []
    invalid_usable = False
    for row in rows:
        if not _truthy(row.get(usable_key)):
            continue
        ratio = _finite(row.get(ratio_key))
        if ratio is None:
            invalid_usable = True
        else:
            ratios.append(ratio)
    if invalid_usable:
        grade = GRADE_C
        reasons.append("missing_or_nonfinite_ratio_in_usable_frame")

    b_fraction = float((contract.get("frame_count_contract") or {}).get("B_minimum_usable_count_fraction") or 0.9)
    b_minimum = math.ceil(required_count * b_fraction)
    if len(ratios) < b_minimum:
        grade = GRADE_C
        reasons.append(f"usable_ratio_count_below_minimum:{len(ratios)}<{b_minimum}")
    elif len(ratios) < required_count:
        grade = _worse(grade, GRADE_B)
        reasons.append(f"usable_ratio_count_below_required:{len(ratios)}<{required_count}")

    ratio_span: float | None = None
    if ratios:
        ratio_span = round(max(ratios) - min(ratios), 12)
        a_limit = float(route["A_ratio_span_max"])
        if ratio_span > a_limit and not math.isclose(ratio_span, a_limit, abs_tol=1e-12):
            if component == "co2":
                b_limit = float(route["B_ratio_span_max"])
                if ratio_span <= b_limit or math.isclose(ratio_span, b_limit, abs_tol=1e-12):
                    grade = _worse(grade, GRADE_B)
                    reasons.append("co2_ratio_span_above_a_within_b")
                else:
                    grade = GRADE_C
                    reasons.append("co2_ratio_span_above_b")
            else:
                grade = _worse(grade, GRADE_B)
                reasons.append("h2o_ratio_span_above_a_diagnostic_only")
    if temporal_evidence.get("cadence_warning") is True and grade != GRADE_C:
        grade = _worse(grade, GRADE_B)
        reasons.append("cadence_warning_grade_capped_at_b")
    if hard_blockers:
        grade = GRADE_C
        reasons.extend(f"point_wide_hard_blocker:{value}" for value in hard_blockers)

    semantics = (contract.get("common_grade_contract") or {}).get(grade) or {}
    return {
        "label": prefix.upper(),
        "prefix": prefix,
        "analyzer_device_id": next(iter(ids)) if len(ids) == 1 else "",
        "grade": grade,
        "ratio_key": ratio_key,
        "ratio_span": ratio_span,
        "ratio_a_tol": route.get("A_ratio_span_max"),
        "ratio_hard_tol": route.get("B_ratio_span_max"),
        "frame_count": len(rows),
        "usable_ratio_count": len(ratios),
        "required_sample_count": required_count,
        **temporal_evidence,
        "reason": ";".join(sorted(set(reasons))) if reasons else "within_production_contract",
        "sample_can_enter_calibration_fit": semantics.get("sample_can_enter_calibration_fit") is True,
        "sample_can_enter_diagnostic_model": semantics.get("sample_can_enter_diagnostic_model") is True,
    }


def _anchor_role(component: str, target: float | None) -> str:
    if component == "co2":
        return "co2_zero_gas_low_concentration_anchor" if target == 0 else "co2_standard_nonzero_point"
    return "h2o_wet_calibration_point"


def _evaluate_candidate(
    *,
    candidate: Mapping[str, Any],
    checks: Mapping[str, Mapping[str, Any]],
    contract: Mapping[str, Any],
    accepted_composite: set[str],
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    component = str(candidate.get("route_kind") or "").strip().lower()
    point_dir = str(Path(str(candidate.get("point_dir") or "")).resolve())
    reasons: list[str] = []
    if component not in {"co2", "h2o"}:
        reasons.append("route_kind_invalid")
    if candidate.get("preflight_ready") is not True:
        reasons.append("candidate_preflight_not_ready")

    samples_path = _bound_artifact(checks, "samples", reasons)
    runtime_path = _bound_artifact(checks, "runtime_config", reasons)
    sidecar_role = "sidecar"
    sidecar_path = _bound_artifact(checks, sidecar_role, reasons)
    frame_qc_path = _bound_artifact(checks, "frame_qc", reasons)
    route_timing_path = _bound_artifact(checks, "route_timing", reasons) if component == "co2" else None
    humidity_path = _bound_artifact(checks, "humidity_reference_review", reasons) if component == "h2o" else None
    hgen_flow_path = _bound_artifact(checks, "hgen_flow_set", reasons) if component == "h2o" else None
    point_timing_path = (
        _bound_artifact(checks, "point_timing_summary", reasons)
        if component == "h2o" or "point_timing_summary" in checks
        else None
    )
    reference_source_path = Path(point_dir) / FORMAL_REFERENCE_SOURCE_RECORD_FILENAME
    if not reference_source_path.is_file():
        reference_source_path = None

    sample_rows = _read_csv(samples_path) if samples_path else []
    runtime = _read_json(runtime_path) if runtime_path else {}
    sidecar = _read_json(sidecar_path) if sidecar_path else {}
    route_timing = _read_json(route_timing_path) if route_timing_path else {}
    humidity_reference = _read_json(humidity_path) if humidity_path else {}
    bound_paths = {
        role: path
        for role, path in {
            "samples": samples_path,
            "frame_qc": frame_qc_path,
            "runtime_config": runtime_path,
            "sidecar": sidecar_path,
            "route_timing": route_timing_path,
            "hgen_flow_set": hgen_flow_path,
            "humidity_reference_review": humidity_path,
            "point_timing_summary": point_timing_path,
            "reference_source": reference_source_path,
        }.items()
        if path is not None
    }
    identity_evidence = _evaluate_evidence_identity(
        point_dir=Path(point_dir),
        component=component,
        sample_rows=sample_rows,
        runtime=runtime,
        sidecar=sidecar,
        route_timing=route_timing,
        bound_paths=bound_paths,
    ) if component in {"co2", "h2o"} else {
        "evidence_identity_status": "fail",
        "evidence_identity_reason_codes": ["route_kind_invalid"],
    }
    reference_source_evidence = _evaluate_reference_source_record(
        Path(point_dir),
        component=component,
        expected_run_id=Path(point_dir).name,
        strict_identity=(
            identity_evidence.get("evidence_identity_mode")
            == "strict_claim_runtime_bundle"
        ),
    ) if component in {"co2", "h2o"} else {
        "reference_source_status": "fail",
        "reference_source_record_valid": False,
        "reference_source_reason_codes": ["route_kind_invalid"],
    }
    hard_blockers = _point_hard_blockers(
        component=component,
        candidate=candidate,
        sample_rows=sample_rows,
        sidecar=sidecar,
        route_timing=route_timing,
        humidity_reference=humidity_reference,
    ) if component in {"co2", "h2o"} else ["route_specific_physical_reference_invalid"]
    if identity_evidence.get("evidence_identity_status") != "pass":
        hard_blockers = sorted(set(hard_blockers + ["evidence_identity_invalid"]))
    if reference_source_evidence.get("reference_source_status") == "fail":
        hard_blockers = sorted(set(hard_blockers + ["reference_source_invalid"]))
    if reasons:
        hard_blockers = sorted(set(hard_blockers + ["sample_window_missing"]))

    required_count = _required_count(runtime, component, contract) if component in {"co2", "h2o"} else 10
    target = _route_target(component, sample_rows, sidecar) if component in {"co2", "h2o"} else None
    temporal_evidence = evaluate_component_qc_temporal_window(
        [row.get("sample_ts") for row in sample_rows],
        expected_interval_s=(
            _expected_sample_interval_s(runtime, component)
            if component in {"co2", "h2o"}
            else None
        ),
        required_count=required_count,
        contract=contract,
    )
    analyzer_rows = [
        _evaluate_analyzer(
            component=component,
            prefix=prefix,
            rows=sample_rows,
            required_count=required_count,
            hard_blockers=hard_blockers,
            temporal_evidence=temporal_evidence,
            contract=contract,
        )
        for prefix in _analyzer_prefixes(sample_rows, component)
    ] if component in {"co2", "h2o"} else []
    if not analyzer_rows:
        reasons.append("active_analyzers_missing")

    selected = point_dir.casefold() in accepted_composite
    source_hashes = {
        **{
            f"source_{role}_sha256": _sha256(path)
            for role, path in sorted(bound_paths.items())
        },
        "contract_sha256": _payload_sha256(contract),
    }
    source_hashes.setdefault(
        "source_reference_source_sha256",
        str(reference_source_evidence.get("reference_source_sha256") or ""),
    )
    qc_rows: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []
    for analyzer in analyzer_rows:
        qc = {
            "component": component,
            "point_name": candidate.get("point_name"),
            "point_dir": point_dir,
            "target_value": target,
            "anchor_role": _anchor_role(component, target),
            "accepted_composite_member": selected,
            **analyzer,
            **identity_evidence,
            **reference_source_evidence,
            **source_hashes,
        }
        qc_rows.append(qc)
        if analyzer["grade"] == GRADE_C:
            decision = "reject_by_component_qc"
        elif analyzer["grade"] == GRADE_B:
            decision = "diagnostic_only_by_component_qc"
        elif component == "co2" and not selected:
            decision = "diagnostic_only_not_selected_by_accepted_composite"
        else:
            decision = "diagnostic_only_no_continuous_route_attestation"
        decisions.append(
            {
                "component": component,
                "point_name": candidate.get("point_name"),
                "point_dir": point_dir,
                "target_value": target,
                "anchor_role": qc["anchor_role"],
                "analyzer_prefix": analyzer["prefix"],
                "analyzer_device_id": analyzer["analyzer_device_id"],
                "component_qc_grade": analyzer["grade"],
                "accepted_composite_member": selected,
                "physical_decision": decision,
                "supersede_reason": (
                    "accepted_composite_selection_is_diagnostic_only"
                    if component == "co2" and selected
                    else "not_selected_by_accepted_composite_manifest"
                    if component == "co2"
                    else "no_continuous_mature_h2o_13_point_root"
                ),
                "calibration_fit_allowed": False,
                "diagnostic_model_allowed": analyzer["sample_can_enter_diagnostic_model"],
                "reason": analyzer["reason"],
            }
        )

    point_row = {
        "component": component,
        "point_name": candidate.get("point_name"),
        "point_dir": point_dir,
        "target_value": target,
        "accepted_composite_member": selected,
        "analyzer_count": len(analyzer_rows),
        "grade_a_count": sum(row["grade"] == GRADE_A for row in analyzer_rows),
        "grade_b_count": sum(row["grade"] == GRADE_B for row in analyzer_rows),
        "grade_c_count": sum(row["grade"] == GRADE_C for row in analyzer_rows),
        "hard_blockers": ";".join(hard_blockers),
        **reference_source_evidence,
        "evaluation_reasons": ";".join(sorted(set(reasons))),
        "evaluation_status": "evaluated" if analyzer_rows and not reasons else "review_required",
    }
    return point_row, qc_rows, decisions


def _fit_matrix_rows(
    qc_rows: Sequence[Mapping[str, Any]], *, continuity_ready: bool
) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in qc_rows:
        key = (
            str(row.get("component") or ""),
            str(row.get("prefix") or ""),
            str(row.get("analyzer_device_id") or ""),
        )
        groups[key].append(row)
    matrix: list[dict[str, Any]] = []
    for (component, prefix, device_id), rows in sorted(groups.items()):
        a_count = sum(row.get("grade") == GRADE_A for row in rows)
        diagnostic_count = sum(row.get("sample_can_enter_diagnostic_model") is True for row in rows)
        anchors = Counter(str(row.get("anchor_role") or "") for row in rows)
        for point_strategy, strategy_role in POINT_SELECTION_STRATEGIES:
            for model_name, terms in MODEL_FAMILIES:
                reasons: list[str] = []
                if not continuity_ready:
                    reasons.append("continuous_mature_route_attestation_missing")
                if a_count < len(terms):
                    reasons.append(f"a_grade_point_count_below_term_count:{a_count}<{len(terms)}")
                if component == "co2" and anchors.get("co2_zero_gas_low_concentration_anchor", 0) == 0:
                    reasons.append("co2_zero_gas_anchor_missing")
                if component == "h2o":
                    reasons.append("h2o_dry_gas_anchor_missing_from_current_point_packet")
                matrix.append(
                    {
                        "component": component,
                        "analyzer_prefix": prefix,
                        "analyzer_device_id": device_id,
                        "point_selection_strategy": point_strategy,
                        "strategy_role": strategy_role,
                        "model_name": model_name,
                        "model_terms": ";".join(terms),
                        "a_grade_point_count": a_count,
                        "diagnostic_point_count": diagnostic_count,
                        "fit_executed": False,
                        "strategy_status": "ready_for_no_write_fit" if not reasons else "blocked",
                        "recommended_for_write": False,
                        "blocker_codes": ";".join(sorted(set(reasons))),
                        "selection_contract": "min_max_relative_error_then_max_absolute_error_then_term_count_then_condition",
                        "relative_error_target_pct": 1.0 if component == "co2" else 2.0,
                        "s5_s6_policy": "deferred_until_main_chain_post_write_reverify",
                    }
                )
    return matrix


def build_v1_5_production_component_qc_fit_matrix(
    *,
    preflight_json: str | Path,
    contract_json: str | Path,
    legacy_catalog_json: str | Path,
    mature_root_discovery_json: str | Path,
) -> dict[str, Any]:
    """Evaluate bound point windows and emit a no-write 0613 strategy matrix."""

    preflight_path = Path(preflight_json).resolve()
    contract_path = Path(contract_json).resolve()
    catalog_path = Path(legacy_catalog_json).resolve()
    discovery_path = Path(mature_root_discovery_json).resolve()
    preflight = _read_json(preflight_path)
    contract = _read_json(contract_path)
    catalog = _read_json(catalog_path)
    discovery = _read_json(discovery_path)
    global_reasons = validate_v1_5_component_qc_generator_contract(contract)
    if preflight.get("overall_status") != "ready_for_historical_component_qc_generator_preflight_manual_review":
        global_reasons.append("component_qc_preflight_status_invalid")

    accepted_composite = {
        str(Path(str(row.get("point_dir") or "")).resolve()).casefold()
        for row in catalog.get("accepted_composite_manifest_rows") or []
        if isinstance(row, Mapping) and str(row.get("point_dir") or "").strip()
    }
    artifact_groups = _artifact_groups(preflight)
    point_rows: list[dict[str, Any]] = []
    qc_rows: list[dict[str, Any]] = []
    fit_input_rows: list[dict[str, Any]] = []
    if not global_reasons:
        for candidate in preflight.get("candidates") or []:
            if not isinstance(candidate, Mapping):
                global_reasons.append("preflight_candidate_not_object")
                continue
            point_dir = str(Path(str(candidate.get("point_dir") or "")).resolve())
            point, analyzers, decisions = _evaluate_candidate(
                candidate=candidate,
                checks=artifact_groups.get(point_dir, {}),
                contract=contract,
                accepted_composite=accepted_composite,
            )
            point_rows.append(point)
            qc_rows.extend(analyzers)
            fit_input_rows.extend(decisions)

    continuity_ready = (
        catalog.get("continuous_route_attestation_allowed") is True
        and catalog.get("historical_fit_allowed") is True
        and discovery.get("overall_status") == "complete_mature_root_found"
    )
    fit_matrix = _fit_matrix_rows(qc_rows, continuity_ready=continuity_ready)
    fit_ready_rows = [row for row in fit_matrix if row["strategy_status"] == "ready_for_no_write_fit"]
    evaluator_complete = bool(point_rows) and not global_reasons
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": (
            "production_component_qc_evaluated_fit_matrix_blocked_by_continuity"
            if evaluator_complete and not fit_ready_rows
            else "ready_for_0613_no_write_fit_review"
            if evaluator_complete
            else "review_required"
        ),
        "production_component_qc_evaluator_available": True,
        "canonical_0613_strategy_matrix_available": True,
        "production_component_qc_evaluation_complete": evaluator_complete,
        "no_write_fit_evaluation_allowed": bool(fit_ready_rows),
        "production_fit_allowed": False,
        "global_review_reasons": sorted(set(global_reasons)),
        "point_count": len(point_rows),
        "analyzer_qc_row_count": len(qc_rows),
        "fit_input_decision_count": len(fit_input_rows),
        "fit_strategy_row_count": len(fit_matrix),
        "fit_ready_strategy_count": len(fit_ready_rows),
        "grade_counts": dict(sorted(Counter(row["grade"] for row in qc_rows).items())),
        "component_point_counts": dict(sorted(Counter(row["component"] for row in point_rows).items())),
        "continuous_mature_route_attestation_ready": continuity_ready,
        "fit_baseline": "0613",
        "route_baselines": ["0620", "0621"],
        "legacy_point_contract": {"co2": 45, "h2o": 13},
        "physical_contract": {
            "primary_temperature": "analyzer_chamber_temperature_T1",
            "legacy_fit_input": "filtered_ratio_R",
            "pressure_terms": "independent_S9_then_frozen_for_atmospheric_open_flow_fit",
            "co2_zero_gas_role": "CO2_low_concentration_anchor_not_H2O_dry_anchor",
            "h2o_dry_gas_role": "separate_dewpoint_pressure_traceable_anchor",
            "uncalibrated_output_exclusion_forbidden": True,
            "s5_s6": "deferred_final_output_layer",
        },
        "source_bindings": {
            "preflight_json": str(preflight_path),
            "preflight_sha256": _sha256(preflight_path),
            "contract_json": str(contract_path),
            "contract_sha256": _sha256(contract_path),
            "legacy_catalog_json": str(catalog_path),
            "legacy_catalog_sha256": _sha256(catalog_path),
            "mature_root_discovery_json": str(discovery_path),
            "mature_root_discovery_sha256": _sha256(discovery_path),
        },
        "points": point_rows,
        "analyzer_qc": qc_rows,
        "fit_input_decisions": fit_input_rows,
        "fit_strategy_matrix": fit_matrix,
        "next_action": (
            "Bind one continuous 45/13 mature root and a separate traceable H2O dry-gas anchor, then rerun the no-write matrix."
            if not fit_ready_rows
            else "Execute and review the eligible no-write model candidates; this matrix alone never authorizes production fitting or writing."
        ),
        "evidence_source": EVIDENCE_SOURCE,
        "not_real_acceptance_evidence": True,
        "historical_point_directories_written": False,
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_sn_or_device_code": False,
        "connects_postgresql": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
    }


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(str(key))
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def write_v1_5_production_component_qc_fit_matrix(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    out = Path(output_dir).resolve()
    if tuple(part.lower() for part in out.parts[-3:]) != OUTPUT_SUFFIX:
        raise ValueError("output_dir_must_be_production_component_qc_fit_matrix_review_directory")
    out.mkdir(parents=True, exist_ok=True)
    outputs = {
        "json": out / "v1_5_production_component_qc_fit_matrix.json",
        "points_csv": out / "v1_5_production_component_qc_points.csv",
        "analyzer_qc_csv": out / "v1_5_production_component_qc_by_analyzer.csv",
        "fit_inputs_csv": out / "v1_5_0613_fit_input_decisions.csv",
        "fit_matrix_csv": out / "v1_5_0613_fit_strategy_matrix.csv",
        "markdown": out / "V1_5_PRODUCTION_COMPONENT_QC_AND_0613_FIT_MATRIX.md",
    }
    outputs["json"].write_text(json.dumps(model, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    _write_csv(outputs["points_csv"], model.get("points") or [])
    _write_csv(outputs["analyzer_qc_csv"], model.get("analyzer_qc") or [])
    _write_csv(outputs["fit_inputs_csv"], model.get("fit_input_decisions") or [])
    _write_csv(outputs["fit_matrix_csv"], model.get("fit_strategy_matrix") or [])
    lines = [
        "# V1.5 生产 Component-QC 与 0613 拟合策略矩阵",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- point_count: `{model.get('point_count')}`",
        f"- analyzer_qc_row_count: `{model.get('analyzer_qc_row_count')}`",
        f"- grade_counts: `{json.dumps(model.get('grade_counts') or {}, ensure_ascii=False, sort_keys=True)}`",
        f"- fit_strategy_row_count: `{model.get('fit_strategy_row_count')}`",
        f"- fit_ready_strategy_count: `{model.get('fit_ready_strategy_count')}`",
        f"- continuous_mature_route_attestation_ready: `{str(model.get('continuous_mature_route_attestation_ready')).lower()}`",
        f"- production_fit_allowed: `{str(model.get('production_fit_allowed')).lower()}`",
        "- evidence_source: `historical_replay`",
        "- not_real_acceptance_evidence: `true`",
        "",
        "## 物理口径",
        "",
        "- 逐分析仪独立分级；单台不稳不取消其它已合格分析仪的采样资格。",
        "- 旧算法主输入是滤波后比值 R，温度使用每台分析仪自己的腔体温度 T1。",
        "- CO2 零气只承担 CO2 低端锚点角色，不替代 H2O 干气锚点。",
        "- H2O 干气锚点必须单独绑定露点与压力证据。",
        "- S5/S6 是主链写入并独立复验后的最终输出层，不提前吸收主模型问题。",
        "",
        "## 当前结论",
        "",
        f"- {model.get('next_action')}",
        "- 本包只在中央 review 目录生成证据，不回写历史点目录，不计算可写系数。",
        "",
    ]
    outputs["markdown"].write_text("\n".join(lines), encoding="utf-8")
    return outputs


__all__ = [
    "MODEL_FAMILIES",
    "POINT_SELECTION_STRATEGIES",
    "SCHEMA",
    "build_v1_5_production_component_qc_fit_matrix",
    "write_v1_5_production_component_qc_fit_matrix",
]
