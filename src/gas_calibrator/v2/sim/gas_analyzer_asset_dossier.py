"""Offline GA-D5 replay of the 0620/0621 asset-dossier gaps."""

from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime
from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Mapping

from gas_calibrator.utils.file_io import write_json as _write_json
from gas_calibrator.validation.metrology.gas_analyzer_asset_dossier import (
    analyze_gas_analyzer_asset_dossier,
    build_gas_analyzer_asset_dossier_acceptance,
)



DEFAULT_GAS_ANALYZER_ASSET_DOSSIER_CONTRACT_PATH = (
    Path(__file__).resolve().parents[1]
    / "configs"
    / "metrology"
    / "gas_analyzer_asset_dossier_contract_v1.json"
)
DEFAULT_GA_D5_OBSERVED_GAP_FIXTURE_PATH = (
    Path(__file__).resolve().parents[1]
    / "configs"
    / "metrology"
    / "ga_d5_0620_0621_observed_gap_fixture_v1.json"
)


def load_gas_analyzer_asset_dossier_contract(
    path: str | Path = DEFAULT_GAS_ANALYZER_ASSET_DOSSIER_CONTRACT_PATH,
) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if payload.get("schema_version") != "gas_analyzer_asset_dossier_contract_v1":
        raise ValueError("unexpected GA-D5 asset-dossier contract schema")
    if payload.get("scope") != "historical_0620_0621_asset_dossier_replay_only":
        raise ValueError("GA-D5 scope must remain historical replay only")
    try:
        date.fromisoformat(str(payload.get("assessment_date") or ""))
    except ValueError as exc:
        raise ValueError("GA-D5 assessment_date must be ISO-8601") from exc
    boundary = dict(payload.get("evidence_boundary") or {})
    required_boundary = {
        "evidence_source": "replay",
        "not_real_acceptance_evidence": True,
        "acceptance_level": "historical_asset_dossier_readiness",
        "promotion_state": "blocked",
        "real_execution_allowed": False,
        "device_io_allowed": False,
        "coefficient_fit_allowed": False,
        "coefficient_write_allowed": False,
        "database_write_allowed": False,
        "real_primary_latest_refresh_allowed": False,
    }
    for key, expected in required_boundary.items():
        if boundary.get(key) != expected:
            raise ValueError(f"GA-D5 contract must set {key}={expected!r}")
    method = dict(payload.get("method_contract") or {})
    required_method = {
        "mature_route_contract_id": "0613_0620_0621_mature_v1_5_legacy_ratio",
        "production_default_profile_id": "legacy_ratio_production",
        "absorption_profile_role": "shadow_review_only",
        "migration_route_forbidden": True,
        "v1_old_fit_route_forbidden": True,
        "v2_execution_route_forbidden": True,
        "formal_calibration_sample_hz": 1,
        "average1": 49,
        "average2": 49,
        "pressure_first_senco9_required": True,
        "co2_zero_separate_from_h2o_dry": True,
        "h2o_reference_requires_actual_dewpoint_and_pressure": True,
        "filtered_ratio_required": True,
    }
    if method != required_method:
        raise ValueError("GA-D5 must retain the mature 0613/0620/0621 method contract")
    _validate_observed_baseline(dict(payload.get("observed_baseline") or {}))

    roles = [str(item) for item in payload.get("required_dossier_roles") or []]
    expected_roles = {
        "co2_zero_gas",
        "co2_standard_gas_series",
        "h2o_dewpoint_reference",
        "digital_pressure_reference",
        "temperature_reference",
        "flow_reference",
        "timebase_reference",
    }
    if set(roles) != expected_roles or len(roles) != len(expected_roles):
        raise ValueError("GA-D5 must retain all reference dossier roles")
    cardinality = dict(payload.get("role_cardinality") or {})
    if set(cardinality) != expected_roles:
        raise ValueError("GA-D5 cardinality must cover every dossier role")
    for role in expected_roles:
        expected = 10 if role == "co2_standard_gas_series" else 1
        limits = dict(cardinality.get(role) or {})
        if limits != {"minimum": expected, "maximum": expected}:
            raise ValueError(f"GA-D5 must retain controlled cardinality for {role}")

    common_fields = {str(item) for item in payload.get("required_common_fields") or []}
    expected_common_fields = {
        "asset_id",
        "asset_role",
        "asset_type",
        "manufacturer",
        "model",
        "serial_number",
        "status",
        "certificate_id",
        "certificate_document_sha256",
        "certificate_issue_date",
        "certificate_valid_until",
        "traceability_chain",
        "standard_uncertainty",
        "expanded_uncertainty",
        "coverage_factor",
        "uncertainty_unit",
        "calibration_scope",
        "scope_basis_sha256",
        "plan_coverage_complete",
        "covariance_treatment",
    }
    if common_fields != expected_common_fields:
        raise ValueError("GA-D5 must retain all certificate documentary fields")
    gas_fields = {str(item) for item in payload.get("required_gas_fields") or []}
    if gas_fields != {
        "cylinder_serial_number",
        "nominal_value",
        "certified_value",
        "value_unit",
        "balance_gas",
        "gas_matrix",
        "preparation_method",
    }:
        raise ValueError("GA-D5 must retain all gas identity fields")
    if set(payload.get("allowed_covariance_treatments") or []) != {
        "independent_physical_asset",
        "covariance_included",
    }:
        raise ValueError("GA-D5 must retain controlled covariance treatments")

    source_roles = [
        str(item) for item in payload.get("required_source_artifacts") or []
    ]
    expected_source_hashes = dict(payload.get("observed_source_sha256") or {})
    if (
        set(source_roles) != set(expected_source_hashes)
        or len(source_roles) != len(expected_source_hashes)
        or any(not _valid_sha256(value) for value in expected_source_hashes.values())
    ):
        raise ValueError("GA-D5 must retain locked source artifact hashes")
    interpretation = dict(payload.get("interpretation") or {})
    required_interpretation = {
        "historical_measurement_presence_is_dossier_readiness": False,
        "recovered_certified_values_are_certificate_documents": False,
        "device_output_certificates_are_reference_asset_certificates": False,
        "co2_zero_and_h2o_dry_are_distinct": True,
        "h2o_generator_nominal_is_primary_reference": False,
        "complete_dossier_is_execution_authorization": False,
        "complete_dossier_is_real_acceptance": False,
    }
    if interpretation != required_interpretation:
        raise ValueError("GA-D5 must retain historical evidence interpretation")
    return payload


def load_ga_d5_observed_gap_fixture(
    path: str | Path = DEFAULT_GA_D5_OBSERVED_GAP_FIXTURE_PATH,
    *,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if payload.get("schema_version") != "ga_d5_0620_0621_observed_gap_fixture_v1":
        raise ValueError("unexpected GA-D5 observed-gap fixture schema")
    if payload.get("source_mode") != "historical_registry_replay":
        raise ValueError("GA-D5 fixture must remain historical_registry_replay")
    active_contract = (
        dict(contract)
        if contract is not None
        else load_gas_analyzer_asset_dossier_contract()
    )
    if dict(payload.get("method_contract") or {}) != dict(
        active_contract.get("method_contract") or {}
    ):
        raise ValueError("GA-D5 fixture method contract drifted")
    if dict(payload.get("observed_state") or {}) != dict(
        active_contract.get("observed_baseline") or {}
    ):
        raise ValueError("GA-D5 fixture observed baseline drifted")
    if dict(payload.get("execution_boundary") or {}) != {
        "real_execution_requested": False,
        "device_io_requested": False,
        "coefficient_fit_requested": False,
        "coefficient_write_requested": False,
        "database_write_requested": False,
        "real_primary_latest_refresh_requested": False,
    }:
        raise ValueError("GA-D5 fixture must remain offline and no-write")
    source_hashes = {
        str(item.get("role") or ""): str(item.get("sha256") or "")
        for item in payload.get("source_artifacts") or []
    }
    if source_hashes != dict(active_contract.get("observed_source_sha256") or {}):
        raise ValueError("GA-D5 fixture source hashes drifted")
    return payload


def generate_complete_asset_dossier_fixture(
    observed_snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    """Return a hypothetical complete documentary package for gate tests only."""

    payload = deepcopy(dict(observed_snapshot))
    recovered = {
        int(float(item.get("nominal_value"))): float(item.get("certified_value"))
        for item in payload.get("asset_records") or []
        if item.get("asset_role") == "co2_standard_gas_series"
    }
    records = [
        _complete_gas_asset(
            role="co2_zero_gas",
            index=0,
            nominal=0.0,
            certified=0.15,
        )
    ]
    records.extend(
        _complete_gas_asset(
            role="co2_standard_gas_series",
            index=index,
            nominal=float(nominal),
            certified=certified,
        )
        for index, (nominal, certified) in enumerate(
            sorted(recovered.items()),
            start=1,
        )
    )
    for index, (role, asset_type, unit) in enumerate(
        (
            ("h2o_dewpoint_reference", "dewpoint_meter", "deg_c"),
            ("digital_pressure_reference", "digital_pressure_gauge", "kpa"),
            ("temperature_reference", "reference_thermometer", "deg_c"),
            ("flow_reference", "reference_flow_meter", "lpm"),
            ("timebase_reference", "reference_timebase", "s"),
        ),
        start=20,
    ):
        records.append(
            _complete_common_asset(
                role=role,
                asset_type=asset_type,
                index=index,
                uncertainty_unit=unit,
            )
        )
    payload["asset_records"] = records
    return payload


def build_gas_analyzer_asset_dossier_offline_report(
    *,
    report_root: Path,
    run_name: str = "ga_d5_0620_0621_asset_dossier_gaps",
    contract_path: str | Path = DEFAULT_GAS_ANALYZER_ASSET_DOSSIER_CONTRACT_PATH,
    fixture_path: str | Path = DEFAULT_GA_D5_OBSERVED_GAP_FIXTURE_PATH,
) -> dict[str, Any]:
    contract = load_gas_analyzer_asset_dossier_contract(contract_path)
    snapshot = load_ga_d5_observed_gap_fixture(
        fixture_path,
        contract=contract,
    )
    report_dir = Path(report_root) / str(run_name)
    report_dir.mkdir(parents=True, exist_ok=True)
    readiness = analyze_gas_analyzer_asset_dossier(snapshot, contract=contract)
    acceptance = build_gas_analyzer_asset_dossier_acceptance(
        readiness,
        contract=contract,
    )
    status = (
        "EXPECTED_GAPS"
        if acceptance["historical_baseline_consistent"]
        and acceptance["expected_gaps_observed"]
        and not acceptance["asset_documentary_ready"]
        and not acceptance["current_prerequisites_ready"]
        else "MISMATCH"
    )
    inputs_path = _write_json(
        report_dir / "gas_analyzer_asset_dossier_replay_inputs.json",
        {
            "artifact_type": "gas_analyzer_asset_dossier_replay_inputs",
            "artifact_role": "execution_summary",
            "schema_version": "gas_analyzer_asset_dossier_replay_inputs_v1",
            "evidence_source": "replay",
            "evidence_state": "historical_registry_gap_replay",
            "not_real_acceptance_evidence": True,
            "promotion_state": "blocked",
            "snapshot": snapshot,
        },
    )
    report = {
        "artifact_type": "gas_analyzer_asset_dossier_report",
        "artifact_role": "diagnostic_analysis",
        "schema_version": "gas_analyzer_asset_dossier_report_v1",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": status,
        "compare_status": status,
        "evidence_source": "replay",
        "evidence_state": "historical_registry_gap_replay",
        "not_real_acceptance_evidence": True,
        "acceptance_level": "historical_asset_dossier_readiness",
        "promotion_state": "blocked",
        "contract_id": contract.get("contract_id"),
        "snapshot_id": readiness.get("snapshot_id"),
        "historical_baseline_consistent": acceptance["historical_baseline_consistent"],
        "asset_documentary_ready": acceptance["asset_documentary_ready"],
        "current_prerequisites_ready": acceptance["current_prerequisites_ready"],
        "expected_gaps_observed": acceptance["expected_gaps_observed"],
        "ready_for_real_execution": False,
        "execution_authorization_status": "not_requested",
        "real_acceptance_status": "blocked",
        "device_io_status": "not_attempted",
        "coefficient_fit_status": "not_attempted",
        "coefficient_writeback_status": "not_attempted",
        "database_write_status": "not_attempted",
        "real_primary_latest_refresh_status": "not_attempted",
        "readiness": readiness,
        "acceptance": acceptance,
        "artifacts": {"execution_summary": str(inputs_path)},
        "boundary_note": acceptance["boundary_note"],
    }
    report_json = _write_json(
        report_dir / "gas_analyzer_asset_dossier_report.json",
        report,
    )
    report_markdown = report_dir / "gas_analyzer_asset_dossier_report.md"
    report_markdown.write_text(_format_markdown(report), encoding="utf-8")
    report["artifacts"].update(
        {
            "report_json": str(report_json),
            "report_markdown": str(report_markdown),
        }
    )
    _write_json(report_json, report)
    return {
        "status": status,
        "compare_status": status,
        "report_dir": str(report_dir),
        "report_json": str(report_json),
        "report_markdown": str(report_markdown),
        "execution_summary": str(inputs_path),
        "report": report,
    }


def _validate_observed_baseline(baseline: dict[str, Any]) -> None:
    expected_scalar = {
        "co2_0620_expected_point_count": 45,
        "co2_0620_accepted_point_count": 45,
        "co2_0620_warning_point_count": 2,
        "co2_0620_missing_or_reject_count": 0,
        "co2_0621_incomplete_zero_attempt_count": 3,
        "h2o_historical_device_count": 6,
        "h2o_historical_blocked_device_count": 3,
        "recovered_co2_asset_count": 10,
        "recovered_co2_formal_fit_checked_rows": 228,
        "recovered_co2_formal_fit_matched_rows": 228,
        "recovered_co2_certificate_documents_linked": 0,
        "formally_authorized_requirement_asset_count": 0,
        "environment_object_count": 11,
        "environment_current_level_id": "Q0",
        "environment_required_level_id": "Q4",
        "actual_r0_result_import_count": 0,
    }
    for key, expected in expected_scalar.items():
        if baseline.get(key) != expected:
            raise ValueError(f"GA-D5 observed baseline must retain {key}")
    if list(baseline.get("co2_0621_completed_entry_points") or []) != [
        {"temperature_c": 40.0, "target_ppm": 0.0},
        {"temperature_c": 40.0, "target_ppm": 400.0},
    ]:
        raise ValueError("GA-D5 must retain the two completed 0621 entry points")
    expected_values = {
        "100": 99.91,
        "200": 199.8,
        "300": 300.36,
        "400": 399.67,
        "500": 500.13,
        "600": 599.54,
        "700": 699.59,
        "800": 800.59,
        "900": 901.78,
        "1000": 1000.22,
    }
    if dict(baseline.get("recovered_co2_certified_values_ppm") or {}) != (
        expected_values
    ):
        raise ValueError("GA-D5 must retain recovered CO2 certificate values")


def _complete_gas_asset(
    *,
    role: str,
    index: int,
    nominal: float,
    certified: float,
) -> dict[str, Any]:
    row = _complete_common_asset(
        role=role,
        asset_type="co2_zero_gas" if role == "co2_zero_gas" else "co2_standard_gas",
        index=index,
        uncertainty_unit="ppm",
    )
    row.update(
        {
            "cylinder_serial_number": f"CYL-{index:03d}",
            "nominal_value": nominal,
            "certified_value": certified,
            "value_unit": "ppm",
            "balance_gas": "synthetic_air",
            "gas_matrix": "air_like",
            "preparation_method": "gravimetric",
        }
    )
    return row


def _complete_common_asset(
    *,
    role: str,
    asset_type: str,
    index: int,
    uncertainty_unit: str,
) -> dict[str, Any]:
    asset_id = f"COMPLETE-{role.upper()}-{index:03d}"
    digest = sha256(asset_id.encode("utf-8")).hexdigest()
    return {
        "asset_id": asset_id,
        "asset_role": role,
        "asset_type": asset_type,
        "manufacturer": "fixture-manufacturer",
        "model": "fixture-model",
        "serial_number": f"SN-{index:03d}",
        "status": "active",
        "certificate_id": f"CERT-{index:03d}",
        "certificate_document_sha256": digest,
        "certificate_issue_date": "2026-01-01",
        "certificate_valid_until": "2027-01-01",
        "traceability_chain": "SI-traceable synthetic fixture",
        "standard_uncertainty": 0.1,
        "expanded_uncertainty": 0.2,
        "coverage_factor": 2.0,
        "uncertainty_unit": uncertainty_unit,
        "calibration_scope": {"plan_coverage": "complete"},
        "scope_basis_sha256": digest,
        "plan_coverage_complete": True,
        "covariance_treatment": "independent_physical_asset",
    }


def _format_markdown(report: Mapping[str, Any]) -> str:
    acceptance = dict(report.get("acceptance") or {})
    readiness = dict(report.get("readiness") or {})
    facts = dict(readiness.get("historical_facts") or {})
    co2_0620 = dict(facts.get("co2_0620") or {})
    co2_0621 = dict(facts.get("co2_0621") or {})
    recovered = dict(facts.get("recovered_co2") or {})
    asset_dossier = dict(readiness.get("asset_dossier") or {})
    blockers = list(acceptance.get("blocking_reasons") or [])
    lines = [
        "# GA-D5 0620/0621 计量资产资料包准备度",
        "",
        "> 历史注册表只读回放；不启动真实台架、不连接 COM、不写设备、系数或数据库。",
        "",
        "## 结论",
        "",
        f"- 状态：`{report.get('status')}`",
        f"- 历史基线一致：`{report.get('historical_baseline_consistent')}`",
        f"- 参考资产资料完整：`{report.get('asset_documentary_ready')}`",
        f"- 当前前置条件完整：`{report.get('current_prerequisites_ready')}`",
        f"- 真实执行就绪：`{report.get('ready_for_real_execution')}`",
        f"- 真实验收：`{report.get('real_acceptance_status')}`",
        "",
        "历史测量存在不等于计量证书资料完整。本报告按预期保留阻断，不能用 45 点完整性替代证书、溯源和当前机器证据。",
        "",
        "## 0620/0621 已确认事实",
        "",
        (
            f"- 0620 CO2：接受 `{co2_0620.get('accepted_point_count')}/"
            f"{co2_0620.get('expected_point_count')}` 点，警告 "
            f"`{co2_0620.get('warning_point_count')}` 点。"
        ),
        (
            f"- 0621 CO2：完成入口点 `{len(co2_0621.get('completed_entry_points') or [])}`，"
            f"未形成完整结论的零点尝试 `{co2_0621.get('incomplete_zero_attempt_count')}`。"
        ),
        (
            f"- 已恢复非零 CO2 实际值 `{recovered.get('asset_count')}` 个，"
            f"历史使用匹配 `{recovered.get('formal_fit_matched_rows')}/"
            f"{recovered.get('formal_fit_checked_rows')}`。"
        ),
        (f"- 已关联原始标气证书：`{recovered.get('certificate_documents_linked')}`。"),
        "",
        "## 资料缺口",
        "",
        f"- 缺失角色：`{', '.join(asset_dossier.get('missing_roles') or []) or '无'}`",
    ]
    lines.extend(f"- `{blocker}`" for blocker in blockers)
    lines.extend(
        [
            "",
            "## 固定边界",
            "",
            "- CO2 零气与 H2O 干气/露点参考保持独立。",
            "- H2O 参考必须由实际露点与实际压力形成，湿度发生器标称值不能替代。",
            "- 成熟 V1.5 legacy ratio 仍是生产默认，吸收比算法仅作 shadow review。",
            "- 即使资料未来补齐，也仍需独立执行授权、当前机器读回和 real acceptance。",
            "",
        ]
    )
    return "\n".join(lines)


def _valid_sha256(value: Any) -> bool:
    text = str(value or "").lower()
    return len(text) == 64 and all(
        character in "0123456789abcdef" for character in text
    )


__all__ = [
    "DEFAULT_GA_D5_OBSERVED_GAP_FIXTURE_PATH",
    "DEFAULT_GAS_ANALYZER_ASSET_DOSSIER_CONTRACT_PATH",
    "build_gas_analyzer_asset_dossier_offline_report",
    "generate_complete_asset_dossier_fixture",
    "load_ga_d5_observed_gap_fixture",
    "load_gas_analyzer_asset_dossier_contract",
]
