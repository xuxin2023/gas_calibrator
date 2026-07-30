"""Historical asset-dossier readiness analysis for the 0620/0621 gas data."""


from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date
from math import isfinite
import re
from typing import Any, Iterable, Mapping

from gas_calibrator.utils.converters import finite_float as _finite_or_none


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_EPSILON = 1e-9


def analyze_gas_analyzer_asset_dossier(
    snapshot: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    """Analyze a replay snapshot without treating historical facts as acceptance."""

    source = dict(snapshot)
    assets = [dict(item) for item in source.get("asset_records") or []]
    source_artifacts = [dict(item) for item in source.get("source_artifacts") or []]
    observed_state = dict(source.get("observed_state") or {})
    expected_state = dict(contract.get("observed_baseline") or {})
    method_contract = dict(source.get("method_contract") or {})
    expected_method_contract = dict(contract.get("method_contract") or {})
    execution_boundary = dict(source.get("execution_boundary") or {})

    source_summary = _analyze_source_artifacts(
        source_artifacts,
        required_roles=contract.get("required_source_artifacts"),
    )
    asset_summary = _analyze_asset_records(
        assets,
        contract=contract,
        assessment_date=str(contract.get("assessment_date") or ""),
    )
    recovered_values = _co2_value_map(assets)
    expected_recovered_values = {
        str(key): float(value)
        for key, value in dict(
            expected_state.get("recovered_co2_certified_values_ppm") or {}
        ).items()
    }
    historical_facts = {
        "co2_0620": {
            "expected_point_count": observed_state.get("co2_0620_expected_point_count"),
            "accepted_point_count": observed_state.get("co2_0620_accepted_point_count"),
            "warning_point_count": observed_state.get("co2_0620_warning_point_count"),
            "missing_or_reject_count": observed_state.get(
                "co2_0620_missing_or_reject_count"
            ),
        },
        "co2_0621": {
            "completed_entry_points": list(
                observed_state.get("co2_0621_completed_entry_points") or []
            ),
            "incomplete_zero_attempt_count": observed_state.get(
                "co2_0621_incomplete_zero_attempt_count"
            ),
        },
        "h2o_0620": {
            "historical_device_count": observed_state.get(
                "h2o_historical_device_count"
            ),
            "historical_blocked_device_count": observed_state.get(
                "h2o_historical_blocked_device_count"
            ),
        },
        "recovered_co2": {
            "asset_count": observed_state.get("recovered_co2_asset_count"),
            "formal_fit_checked_rows": observed_state.get(
                "recovered_co2_formal_fit_checked_rows"
            ),
            "formal_fit_matched_rows": observed_state.get(
                "recovered_co2_formal_fit_matched_rows"
            ),
            "certificate_documents_linked": observed_state.get(
                "recovered_co2_certificate_documents_linked"
            ),
            "certified_values_ppm": recovered_values,
        },
        "current_governance": {
            "formally_authorized_requirement_asset_count": observed_state.get(
                "formally_authorized_requirement_asset_count"
            ),
            "environment_object_count": observed_state.get("environment_object_count"),
            "environment_current_level_id": observed_state.get(
                "environment_current_level_id"
            ),
            "environment_required_level_id": observed_state.get(
                "environment_required_level_id"
            ),
            "actual_r0_result_import_count": observed_state.get(
                "actual_r0_result_import_count"
            ),
        },
    }
    return {
        "artifact_type": "gas_analyzer_asset_dossier_readiness",
        "artifact_role": "diagnostic_analysis",
        "schema_version": "gas_analyzer_asset_dossier_readiness_v1",
        "evidence_source": "replay",
        "evidence_state": "historical_registry_gap_replay",
        "not_real_acceptance_evidence": True,
        "acceptance_level": "historical_asset_dossier_readiness",
        "promotion_state": "blocked",
        "snapshot_schema_version": str(source.get("schema_version") or ""),
        "snapshot_id": str(source.get("snapshot_id") or ""),
        "snapshot_as_of": str(source.get("as_of") or ""),
        "source_mode": str(source.get("source_mode") or ""),
        "method_contract": method_contract,
        "method_contract_matches": method_contract == expected_method_contract,
        "execution_boundary": execution_boundary,
        "source_artifacts": source_summary,
        "historical_facts": historical_facts,
        "observed_state_matches_locked_baseline": observed_state == expected_state,
        "recovered_co2_values_match_locked_baseline": (
            recovered_values == expected_recovered_values
        ),
        "asset_dossier": asset_summary,
        "status": "ok" if source and source_artifacts else "incomplete",
        "ready_for_real_execution": False,
    }


def build_gas_analyzer_asset_dossier_acceptance(
    readiness: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    """Build separate historical-integrity and documentary-readiness gates."""

    sources = dict(readiness.get("source_artifacts") or {})
    assets = dict(readiness.get("asset_dossier") or {})
    facts = dict(readiness.get("historical_facts") or {})
    recovered = dict(facts.get("recovered_co2") or {})
    governance = dict(facts.get("current_governance") or {})
    expected = dict(contract.get("observed_baseline") or {})

    baseline_gates = [
        _gate(
            "analysis_complete",
            readiness.get("status") == "ok",
            readiness.get("status"),
            "ok",
        ),
        _gate(
            "snapshot_schema_version",
            readiness.get("snapshot_schema_version")
            == "ga_d5_0620_0621_observed_gap_fixture_v1",
            readiness.get("snapshot_schema_version"),
            "ga_d5_0620_0621_observed_gap_fixture_v1",
        ),
        _gate(
            "historical_replay_source_mode",
            readiness.get("source_mode") == "historical_registry_replay",
            readiness.get("source_mode"),
            "historical_registry_replay",
        ),
        _gate(
            "mature_method_contract_locked",
            readiness.get("method_contract_matches") is True,
            readiness.get("method_contract"),
            contract.get("method_contract"),
        ),
        _gate(
            "offline_no_io_no_write_scope",
            dict(readiness.get("execution_boundary") or {})
            == {
                "real_execution_requested": False,
                "device_io_requested": False,
                "coefficient_fit_requested": False,
                "coefficient_write_requested": False,
                "database_write_requested": False,
                "real_primary_latest_refresh_requested": False,
            },
            readiness.get("execution_boundary"),
            {"all_execution_and_write_requests": False},
        ),
        _gate(
            "source_artifact_roles_complete",
            sources.get("roles_complete") is True,
            {
                "missing": sources.get("missing_roles"),
                "duplicate": sources.get("duplicate_roles"),
                "unexpected": sources.get("unexpected_roles"),
            },
            {"missing": [], "duplicate": [], "unexpected": []},
        ),
        _gate(
            "source_artifact_hashes_valid",
            sources.get("hashes_valid") is True,
            sources.get("invalid_hash_roles"),
            [],
        ),
        _gate(
            "observed_state_matches_locked_baseline",
            readiness.get("observed_state_matches_locked_baseline") is True,
            readiness.get("observed_state_matches_locked_baseline"),
            True,
        ),
        _gate(
            "recovered_co2_values_match_locked_baseline",
            readiness.get("recovered_co2_values_match_locked_baseline") is True,
            recovered.get("certified_values_ppm"),
            expected.get("recovered_co2_certified_values_ppm"),
        ),
    ]
    asset_gates = [
        _gate(
            "dossier_roles_complete",
            assets.get("roles_complete") is True,
            {
                "missing": assets.get("missing_roles"),
                "cardinality": assets.get("invalid_cardinality_roles"),
                "unexpected": assets.get("unexpected_roles"),
            },
            {"missing": [], "cardinality": [], "unexpected": []},
        ),
        _gate(
            "certificate_documentary_fields_complete",
            assets.get("common_fields_complete") is True,
            assets.get("missing_common_fields_by_role"),
            {},
        ),
        _gate(
            "certificate_lifecycle_valid",
            assets.get("certificate_lifecycle_valid") is True,
            assets.get("invalid_lifecycle_asset_ids"),
            [],
        ),
        _gate(
            "uncertainty_metadata_valid",
            assets.get("uncertainty_metadata_valid") is True,
            assets.get("invalid_uncertainty_asset_ids"),
            [],
        ),
        _gate(
            "calibration_scope_covers_plan",
            assets.get("calibration_scope_complete") is True,
            assets.get("invalid_scope_asset_ids"),
            [],
        ),
        _gate(
            "gas_identity_fields_complete",
            assets.get("gas_fields_complete") is True,
            assets.get("missing_gas_fields_by_role"),
            {},
        ),
        _gate(
            "asset_identity_unique",
            assets.get("asset_identity_unique") is True,
            assets.get("duplicate_asset_ids"),
            [],
        ),
        _gate(
            "shared_asset_covariance_controlled",
            assets.get("covariance_treatment_valid") is True,
            assets.get("invalid_covariance_asset_ids"),
            [],
        ),
    ]
    context_gates = [
        _gate(
            "formal_requirement_assets_authorized",
            int(governance.get("formally_authorized_requirement_asset_count") or 0) > 0,
            governance.get("formally_authorized_requirement_asset_count"),
            "greater_than_zero",
        ),
        _gate(
            "environment_machine_evidence_q4",
            governance.get("environment_object_count")
            == expected.get("environment_object_count")
            and governance.get("environment_current_level_id")
            == governance.get("environment_required_level_id")
            == "Q4",
            {
                "count": governance.get("environment_object_count"),
                "current": governance.get("environment_current_level_id"),
                "required": governance.get("environment_required_level_id"),
            },
            {
                "count": expected.get("environment_object_count"),
                "current": "Q4",
                "required": "Q4",
            },
        ),
        _gate(
            "actual_r0_result_imported",
            int(governance.get("actual_r0_result_import_count") or 0) > 0,
            governance.get("actual_r0_result_import_count"),
            "greater_than_zero",
        ),
    ]

    blockers = _build_blockers(assets, governance)
    expected_blockers = {str(item) for item in contract.get("expected_blockers") or []}
    blocker_set = set(blockers)
    baseline_consistent = all(bool(gate.get("passed")) for gate in baseline_gates)
    asset_documentary_ready = all(bool(gate.get("passed")) for gate in asset_gates)
    current_prerequisites_ready = asset_documentary_ready and all(
        bool(gate.get("passed")) for gate in context_gates
    )
    expected_gaps_observed = expected_blockers <= blocker_set
    return {
        "artifact_type": "gas_analyzer_asset_dossier_acceptance",
        "artifact_role": "diagnostic_analysis",
        "schema_version": "gas_analyzer_asset_dossier_acceptance_v1",
        "evidence_source": "replay",
        "not_real_acceptance_evidence": True,
        "acceptance_level": "historical_asset_dossier_readiness",
        "promotion_state": "blocked",
        "historical_baseline_consistent": baseline_consistent,
        "asset_documentary_ready": asset_documentary_ready,
        "current_prerequisites_ready": current_prerequisites_ready,
        "expected_gaps_observed": expected_gaps_observed,
        "ready_for_real_execution": False,
        "execution_authorization_status": "not_requested",
        "real_acceptance_status": "blocked",
        "device_io_status": "not_attempted",
        "write_status": "not_attempted",
        "baseline_gates": baseline_gates,
        "asset_gates": asset_gates,
        "context_gates": context_gates,
        "blocking_reasons": blockers,
        "missing_expected_blockers": sorted(expected_blockers - blocker_set),
        "boundary_note": (
            "本工件只重放 0620/0621 历史事实并检查计量资料缺口；"
            "历史测量存在不等于证书资料完整，资料完整也不构成真实执行授权或真实验收。"
        ),
    }


def _analyze_source_artifacts(
    artifacts: list[dict[str, Any]],
    *,
    required_roles: Any,
) -> dict[str, Any]:
    required = {str(item) for item in required_roles or []}
    role_counts = Counter(str(item.get("role") or "") for item in artifacts)
    observed = set(role_counts)
    missing = sorted(required - observed)
    duplicates = sorted(role for role, count in role_counts.items() if count > 1)
    unexpected = sorted(observed - required)
    invalid_hash_roles = sorted(
        str(item.get("role") or "")
        for item in artifacts
        if not _valid_sha256(item.get("sha256"))
    )
    missing_identity_roles = sorted(
        str(item.get("role") or "")
        for item in artifacts
        if not str(item.get("artifact_id") or "").strip()
        or not str(item.get("record_class") or "").strip()
    )
    return {
        "artifact_count": len(artifacts),
        "required_roles": sorted(required),
        "missing_roles": missing,
        "duplicate_roles": duplicates,
        "unexpected_roles": unexpected,
        "roles_complete": not missing and not duplicates and not unexpected,
        "invalid_hash_roles": invalid_hash_roles,
        "missing_identity_roles": missing_identity_roles,
        "hashes_valid": not invalid_hash_roles and not missing_identity_roles,
    }


def _analyze_asset_records(
    assets: list[dict[str, Any]],
    *,
    contract: Mapping[str, Any],
    assessment_date: str,
) -> dict[str, Any]:
    required_roles = {
        str(item) for item in contract.get("required_dossier_roles") or []
    }
    cardinality = dict(contract.get("role_cardinality") or {})
    common_fields = {str(item) for item in contract.get("required_common_fields") or []}
    gas_fields = {str(item) for item in contract.get("required_gas_fields") or []}
    allowed_covariance = {
        str(item) for item in contract.get("allowed_covariance_treatments") or []
    }
    role_counts = Counter(str(item.get("asset_role") or "") for item in assets)
    observed_roles = set(role_counts)
    missing_roles = sorted(required_roles - observed_roles)
    unexpected_roles = sorted(observed_roles - required_roles)
    invalid_cardinality_roles = []
    for role in sorted(required_roles):
        limits = dict(cardinality.get(role) or {})
        count = role_counts.get(role, 0)
        minimum = int(limits.get("minimum") or 0)
        maximum = int(limits.get("maximum") or 0)
        if count < minimum or count > maximum:
            invalid_cardinality_roles.append(role)

    missing_common: dict[str, set[str]] = defaultdict(set)
    missing_gas: dict[str, set[str]] = defaultdict(set)
    invalid_lifecycle = []
    invalid_uncertainty = []
    invalid_scope = []
    invalid_covariance = []
    assessment_day = _parse_date(assessment_date)
    for asset in assets:
        asset_id = str(asset.get("asset_id") or "")
        role = str(asset.get("asset_role") or "")
        for field in common_fields:
            if not _present(asset.get(field)):
                missing_common[role].add(field)
        issue_day = _parse_date(asset.get("certificate_issue_date"))
        valid_until = _parse_date(asset.get("certificate_valid_until"))
        if (
            assessment_day is None
            or issue_day is None
            or valid_until is None
            or issue_day > assessment_day
            or assessment_day >= valid_until
            or asset.get("status") != "active"
        ):
            invalid_lifecycle.append(asset_id)
        standard = _finite_or_none(asset.get("standard_uncertainty"))
        expanded = _finite_or_none(asset.get("expanded_uncertainty"))
        coverage = _finite_or_none(asset.get("coverage_factor"))
        if (
            standard is None
            or standard <= 0.0
            or expanded is None
            or expanded <= 0.0
            or coverage is None
            or coverage < 1.0
            or abs(expanded - standard * coverage) > max(_EPSILON, expanded * 0.02)
            or not str(asset.get("uncertainty_unit") or "")
        ):
            invalid_uncertainty.append(asset_id)
        if (
            asset.get("plan_coverage_complete") is not True
            or not _present(asset.get("calibration_scope"))
            or not _valid_sha256(asset.get("scope_basis_sha256"))
        ):
            invalid_scope.append(asset_id)
        if asset.get("covariance_treatment") not in allowed_covariance:
            invalid_covariance.append(asset_id)
        if role in {"co2_zero_gas", "co2_standard_gas_series"}:
            for field in gas_fields:
                if not _present(asset.get(field)):
                    missing_gas[role].add(field)

    serial_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for asset in assets:
        serial = str(asset.get("serial_number") or "").strip()
        if serial:
            serial_groups[serial].append(asset)
    for group in serial_groups.values():
        if len(group) <= 1:
            continue
        correlation_ids = {
            str(item.get("correlation_group_id") or "").strip() for item in group
        }
        if (
            correlation_ids == {""}
            or len(correlation_ids) != 1
            or any(
                item.get("covariance_treatment") != "covariance_included"
                for item in group
            )
        ):
            invalid_covariance.extend(str(item.get("asset_id") or "") for item in group)

    asset_id_counts = Counter(str(item.get("asset_id") or "") for item in assets)
    duplicate_asset_ids = sorted(
        asset_id for asset_id, count in asset_id_counts.items() if count > 1
    )
    return {
        "asset_count": len(assets),
        "role_counts": dict(role_counts),
        "missing_roles": missing_roles,
        "unexpected_roles": unexpected_roles,
        "invalid_cardinality_roles": invalid_cardinality_roles,
        "roles_complete": (
            not missing_roles and not unexpected_roles and not invalid_cardinality_roles
        ),
        "missing_common_fields_by_role": {
            role: sorted(fields) for role, fields in sorted(missing_common.items())
        },
        "common_fields_complete": not missing_common,
        "missing_gas_fields_by_role": {
            role: sorted(fields) for role, fields in sorted(missing_gas.items())
        },
        "gas_fields_complete": not missing_gas,
        "invalid_lifecycle_asset_ids": sorted(set(invalid_lifecycle)),
        "certificate_lifecycle_valid": not invalid_lifecycle,
        "invalid_uncertainty_asset_ids": sorted(set(invalid_uncertainty)),
        "uncertainty_metadata_valid": not invalid_uncertainty,
        "invalid_scope_asset_ids": sorted(set(invalid_scope)),
        "calibration_scope_complete": not invalid_scope,
        "duplicate_asset_ids": duplicate_asset_ids,
        "asset_identity_unique": not duplicate_asset_ids,
        "invalid_covariance_asset_ids": sorted(set(invalid_covariance)),
        "covariance_treatment_valid": not invalid_covariance,
    }


def _co2_value_map(assets: Iterable[Mapping[str, Any]]) -> dict[str, float]:
    values: dict[str, float] = {}
    for asset in assets:
        if asset.get("asset_role") != "co2_standard_gas_series":
            continue
        nominal = _finite_or_none(asset.get("nominal_value"))
        certified = _finite_or_none(asset.get("certified_value"))
        if nominal is None or certified is None:
            continue
        values[str(int(nominal)) if nominal.is_integer() else str(nominal)] = certified
    return dict(sorted(values.items(), key=lambda item: float(item[0])))


def _build_blockers(
    assets: Mapping[str, Any],
    governance: Mapping[str, Any],
) -> list[str]:
    blockers = [
        f"{role}:dossier_role_missing" for role in assets.get("missing_roles") or []
    ]
    for role in dict(assets.get("missing_common_fields_by_role") or {}):
        blockers.append(f"{role}:certificate_documentary_fields_incomplete")
    for role in dict(assets.get("missing_gas_fields_by_role") or {}):
        blockers.append(f"{role}:gas_identity_fields_incomplete")
    if int(governance.get("formally_authorized_requirement_asset_count") or 0) <= 0:
        blockers.append("global:formal_requirement_assets_not_authorized")
    if (
        governance.get("environment_current_level_id")
        != governance.get("environment_required_level_id")
        or governance.get("environment_required_level_id") != "Q4"
    ):
        blockers.append("global:environment_machine_evidence_not_q4")
    if int(governance.get("actual_r0_result_import_count") or 0) <= 0:
        blockers.append("global:actual_r0_result_not_imported")
    return sorted(set(blockers))


def _gate(name: str, passed: bool, observed: Any, required: Any) -> dict[str, Any]:
    return {
        "name": name,
        "passed": bool(passed),
        "observed": observed,
        "required": required,
    }


def _present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    if isinstance(value, (int, float)):
        return isfinite(float(value))
    return True


def _valid_sha256(value: Any) -> bool:
    return bool(_SHA256_RE.fullmatch(str(value or "").lower()))


def _parse_date(value: Any) -> date | None:
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


__all__ = [
    "analyze_gas_analyzer_asset_dossier",
    "build_gas_analyzer_asset_dossier_acceptance",
]
