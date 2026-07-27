"""GA-D6B owner-attested operational certificate admission.

The module deliberately separates an operational gate for offline program
progress from the strict original-certificate dossier and real acceptance.
It performs no device I/O and never mutates source evidence.
"""

from __future__ import annotations

from collections import Counter
import csv
from datetime import date, datetime
import json
from pathlib import Path
from typing import Any, Iterable, Mapping

from gas_calibrator.utils.file_io import sha256_file, write_json as _write_json


_CONFIG_ROOT = Path(__file__).resolve().parents[1] / "configs" / "metrology"
DEFAULT_CONTRACT_PATH = (
    _CONFIG_ROOT / "certificate_operational_admission_contract_v1.json"
)
DEFAULT_EVIDENCE_PATH = (
    _CONFIG_ROOT / "ga_d6b_owner_attested_certificate_evidence_v1.json"
)

_BOUNDARY = {
    "evidence_source": "owner_attested_local_documentary_evidence",
    "not_real_acceptance_evidence": True,
    "promotion_state": "blocked",
    "program_progress_scope": "offline_program_development_and_governance",
    "real_execution_allowed": False,
    "device_io_allowed": False,
    "database_write_allowed": False,
    "coefficient_fit_allowed": False,
    "coefficient_write_allowed": False,
    "real_primary_latest_refresh_allowed": False,
    "source_file_mutation_allowed": False,
    "report_output_only": True,
}
_EXECUTION_BOUNDARY = {
    "real_execution_requested": False,
    "device_io_requested": False,
    "database_write_requested": False,
    "coefficient_fit_requested": False,
    "coefficient_write_requested": False,
    "real_primary_latest_refresh_requested": False,
}


def load_certificate_operational_admission_contract(
    path: str | Path = DEFAULT_CONTRACT_PATH,
) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if payload.get("schema_version") != (
        "certificate_operational_admission_contract_v1"
    ):
        raise ValueError("unexpected GA-D6B operational admission contract schema")
    if dict(payload.get("evidence_boundary") or {}) != _BOUNDARY:
        raise ValueError("GA-D6B boundary must remain offline, no-write, and blocked")

    method = dict(payload.get("method_contract") or {})
    required_method = {
        "formal_calibration_sample_hz": 1,
        "average1": 49,
        "average2": 49,
        "pressure_first_senco9_required": True,
        "co2_zero_separate_from_h2o_dry": True,
        "h2o_reference_requires_actual_dewpoint_and_pressure": True,
        "temperature_truth_source": (
            "in_chamber_platinum_resistance_digital_thermometer"
        ),
        "temperature_chamber_setpoint_is_reference": False,
        "flow_source": "dewpoint_instrument_output.flow_lpm",
        "flow_metrology_role": "process_monitor_only",
        "flow_used_in_concentration_fit_or_correction": False,
        "timebase_source": "software_monotonic_clock_plus_analyzer_1hz_stream",
        "timebase_role": "static_sampling_and_stability_window_only",
        "dynamic_response_or_ec_acceptance_in_scope": False,
    }
    for key, expected in required_method.items():
        if method.get(key) != expected:
            raise ValueError(f"GA-D6B method contract must retain {key}")

    layers = dict(payload.get("admission_layers") or {})
    operational = dict(layers.get("operational_certificate_gate") or {})
    strict = dict(layers.get("strict_original_certificate_gate") or {})
    if operational.get("pass_state") != "PASSED_WITH_OWNER_ATTESTATION":
        raise ValueError("GA-D6B operational pass state drifted")
    if operational.get("allows_real_execution") is not False:
        raise ValueError("GA-D6B operational gate cannot authorize real execution")
    if strict.get("must_remain_blocked_in_this_fixture") is not True:
        raise ValueError("GA-D6B strict original-certificate layer must remain blocked")

    interpretation = dict(payload.get("interpretation") or {})
    required_false = {
        "operational_gate_is_strict_original_certificate_gate",
        "operational_gate_is_execution_authorization",
        "operational_gate_is_real_acceptance",
        "photo_count_proves_physical_cylinder_count",
        "owner_attestation_is_original_certificate",
        "chamber_setpoint_is_true_temperature",
        "dewpoint_flow_is_primary_measurand",
        "static_sampling_timebase_is_dynamic_response_traceability",
    }
    if any(interpretation.get(key) is not False for key in required_false):
        raise ValueError("GA-D6B interpretation cannot promote operational evidence")
    if interpretation.get("co2_zero_and_h2o_dry_are_distinct") is not True:
        raise ValueError("GA-D6B must keep CO2 zero and H2O dry gas distinct")
    return payload


def load_owner_attested_certificate_evidence(
    path: str | Path = DEFAULT_EVIDENCE_PATH,
) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if payload.get("schema_version") != (
        "ga_d6b_owner_attested_certificate_evidence_v1"
    ):
        raise ValueError("unexpected GA-D6B owner-attested evidence schema")
    if dict(payload.get("execution_boundary") or {}) != _EXECUTION_BOUNDARY:
        raise ValueError("GA-D6B evidence must not request execution or writes")
    return payload


def verify_documentary_files(
    evidence: Mapping[str, Any],
    *,
    roots: Iterable[str | Path],
) -> dict[str, Any]:
    """Verify locked file names and SHA-256 values without changing sources."""

    root_paths = [Path(item).resolve() for item in roots]
    if not root_paths:
        raise ValueError("at least one evidence root is required")
    records: list[dict[str, Any]] = []
    hash_cache: dict[Path, str] = {}
    for expected in list(evidence.get("documentary_files") or []):
        filename = str(expected.get("filename") or "")
        expected_sha = str(expected.get("sha256") or "").casefold()
        candidates: list[Path] = []
        for root in root_paths:
            if not root.exists():
                continue
            if root.is_file():
                if root.name == filename:
                    candidates.append(root)
                continue
            direct = root / filename
            if direct.is_file():
                candidates.append(direct)
                continue
            candidates.extend(path for path in root.rglob(filename) if path.is_file())

        unique_candidates = sorted(
            {path.resolve() for path in candidates},
            key=lambda item: str(item).casefold(),
        )
        candidate_rows: list[dict[str, Any]] = []
        matched_path: Path | None = None
        for candidate in unique_candidates:
            actual_sha = hash_cache.setdefault(candidate, sha256_file(candidate))
            matched = actual_sha.casefold() == expected_sha
            candidate_rows.append(
                {
                    "source_path": str(candidate),
                    "sha256": actual_sha,
                    "matched": matched,
                }
            )
            if matched and matched_path is None:
                matched_path = candidate
        status = (
            "matched"
            if matched_path is not None
            else "missing"
            if not unique_candidates
            else "sha256_mismatch"
        )
        records.append(
            {
                "filename": filename,
                "evidence_group": expected.get("evidence_group"),
                "expected_sha256": expected_sha,
                "status": status,
                "matched_source_path": (
                    str(matched_path) if matched_path is not None else None
                ),
                "candidates": candidate_rows,
            }
        )

    counts = Counter(str(item.get("status") or "") for item in records)
    return {
        "artifact_type": "certificate_documentary_source_verification",
        "artifact_role": "execution_rows",
        "schema_version": "certificate_documentary_source_verification_v1",
        "verification_mode": "local_source_filename_and_sha256",
        "source_mutation_status": "not_attempted",
        "root_count": len(root_paths),
        "expected_file_count": len(records),
        "matched_file_count": counts["matched"],
        "missing_file_count": counts["missing"],
        "sha256_mismatch_count": counts["sha256_mismatch"],
        "all_files_verified": bool(records) and counts["matched"] == len(records),
        "records": records,
    }


def build_locked_fixture_verification(
    evidence: Mapping[str, Any],
) -> dict[str, Any]:
    """Build deterministic verification input for suite contract self-tests only."""

    records = [
        {
            "filename": item.get("filename"),
            "evidence_group": item.get("evidence_group"),
            "expected_sha256": item.get("sha256"),
            "status": "matched",
            "matched_source_path": None,
            "candidates": [],
        }
        for item in list(evidence.get("documentary_files") or [])
    ]
    return {
        "artifact_type": "certificate_documentary_source_verification",
        "artifact_role": "execution_rows",
        "schema_version": "certificate_documentary_source_verification_v1",
        "verification_mode": "locked_fixture_contract_self_test",
        "source_mutation_status": "not_attempted",
        "root_count": 0,
        "expected_file_count": len(records),
        "matched_file_count": len(records),
        "missing_file_count": 0,
        "sha256_mismatch_count": 0,
        "all_files_verified": bool(records),
        "records": records,
    }


def evaluate_certificate_operational_admission(
    evidence: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
    source_verification: Mapping[str, Any],
) -> dict[str, Any]:
    """Evaluate the narrow operational gate while preserving strict blockers."""

    active_contract = dict(contract)
    active_evidence = dict(evidence)
    _validate_runtime_payloads(active_contract, active_evidence)
    checks: list[dict[str, Any]] = []

    def check(
        gate_id: str,
        passed: bool,
        *,
        evidence_detail: Any,
        failure_reason: str,
        severity: str = "P0",
    ) -> None:
        checks.append(
            {
                "gate_id": gate_id,
                "status": "passed" if passed else "failed",
                "passed": bool(passed),
                "severity": severity,
                "evidence": evidence_detail,
                "failure_reason": "" if passed else failure_reason,
            }
        )

    attestation = dict(active_evidence.get("owner_attestation") or {})
    check(
        "owner_attestation_locked",
        bool(attestation.get("attestation_id"))
        and attestation.get("attested_by_role") == "project_owner"
        and attestation.get("photo_evidence_accepted_for_operational_gate") is True
        and attestation.get("prior_calibration_values_confirmed_for_use") is True,
        evidence_detail=attestation,
        failure_reason="project-owner evidence attestation is incomplete",
    )

    verification = dict(source_verification)
    check(
        "local_source_sha256_verified",
        verification.get("all_files_verified") is True
        and verification.get("source_mutation_status") == "not_attempted",
        evidence_detail={
            "verification_mode": verification.get("verification_mode"),
            "expected_file_count": verification.get("expected_file_count"),
            "matched_file_count": verification.get("matched_file_count"),
            "missing_file_count": verification.get("missing_file_count"),
            "sha256_mismatch_count": verification.get("sha256_mismatch_count"),
        },
        failure_reason="one or more documentary source files are missing or changed",
    )

    group_counts = Counter(
        str(item.get("evidence_group") or "")
        for item in list(active_evidence.get("documentary_files") or [])
    )
    expected_groups = {
        str(key): int(value)
        for key, value in dict(
            active_contract.get("required_documentary_groups") or {}
        ).items()
    }
    check(
        "documentary_groups_complete",
        all(
            group_counts.get(key, 0) == value for key, value in expected_groups.items()
        ),
        evidence_detail={
            "expected_group_counts": expected_groups,
            "observed_group_counts": dict(group_counts),
        },
        failure_reason="documentary evidence group cardinality drifted",
    )

    co2_series = dict(active_evidence.get("co2_standard_gas_series") or {})
    expected_values = dict(
        active_contract.get("expected_owner_attested_co2_values_ppm") or {}
    )
    observed_values = dict(co2_series.get("values_by_nominal_ppm") or {})
    check(
        "owner_attested_co2_values_locked",
        observed_values == expected_values
        and co2_series.get("value_source")
        == "project_owner_attestation_prior_calibration_flow"
        and co2_series.get("photo_bundle_is_automatic_value_authority") is False
        and co2_series.get("one_to_one_certificate_value_binding_complete") is False,
        evidence_detail={
            "value_source": co2_series.get("value_source"),
            "values_by_nominal_ppm": observed_values,
            "one_to_one_certificate_value_binding_complete": co2_series.get(
                "one_to_one_certificate_value_binding_complete"
            ),
        },
        failure_reason="owner-attested ten-point CO2 value contract drifted",
    )

    low_end = dict(active_evidence.get("co2_low_end_anchor") or {})
    check(
        "co2_low_end_anchor_truthful",
        low_end.get("owner_accepted_role") == "low_co2_dry_air_process_anchor"
        and low_end.get("certified_co2_value_ppm") is None
        and low_end.get("certified_co2_uncertainty_ppm") is None
        and low_end.get("formal_co2_zero_certificate_complete") is False
        and low_end.get("must_not_be_used_as_h2o_dewpoint_reference") is True,
        evidence_detail=low_end,
        failure_reason="dry-air evidence was promoted to a certified CO2 zero value",
    )

    separation = dict(active_evidence.get("anchor_separation") or {})
    check(
        "co2_zero_h2o_dry_separated",
        separation.get("co2_zero_and_h2o_dry_are_distinct") is True
        and separation.get("co2_low_end_anchor_role")
        == "low_co2_dry_air_process_anchor"
        and separation.get("h2o_low_end_anchor_role")
        == "dewpoint_and_pressure_backed_dry_gas_point",
        evidence_detail=separation,
        failure_reason="CO2 low-end gas and H2O dry-gas reference were conflated",
    )

    assessment_date = date.fromisoformat(str(active_contract.get("assessment_date")))
    dewpoint = dict(active_evidence.get("h2o_reference") or {})
    dewpoint_due = date.fromisoformat(str(dewpoint.get("valid_until")))
    check(
        "h2o_dewpoint_reference_current",
        dewpoint.get("certificate_id") == "FCDjw25074175"
        and dewpoint.get("serial_number") == "245932001"
        and dewpoint_due >= assessment_date
        and dewpoint.get("actual_dewpoint_and_pressure_required_per_measurement")
        is True
        and dewpoint.get("dry_gas_anchor_is_co2_zero_anchor") is False,
        evidence_detail={
            **dewpoint,
            "days_until_expiry": (dewpoint_due - assessment_date).days,
        },
        failure_reason="dewpoint reference identity, validity, or pressure binding failed",
    )

    pressure = dict(active_evidence.get("pressure_reference") or {})
    pressure_due = date.fromisoformat(str(pressure.get("valid_until")))
    pressure_scope = list(pressure.get("scope_hpa_absolute") or [])
    check(
        "digital_pressure_reference_current",
        pressure.get("certificate_id") == "FRGsz25038057"
        and pressure.get("serial_number") == "118288"
        and pressure_due >= assessment_date
        and pressure_scope == [500.0, 1100.0],
        evidence_detail=pressure,
        failure_reason="digital pressure reference identity, validity, or scope failed",
    )

    temperature = dict(active_evidence.get("temperature_reference") or {})
    chamber = dict(active_evidence.get("temperature_chamber") or {})
    check(
        "in_chamber_platinum_thermometer_is_temperature_truth",
        temperature.get("certificate_id") == "GQJ(C)WD2026-0105"
        and temperature.get("serial_number") == "2021009"
        and temperature.get("placement") == "inside_temperature_chamber"
        and temperature.get("truth_source") == "platinum_resistance_digital_thermometer"
        and temperature.get("temperature_chamber_setpoint_used_as_truth") is False
        and chamber.get("role") == "environment_control_only"
        and chamber.get("setpoint_used_as_temperature_reference") is False,
        evidence_detail={
            "temperature_reference": temperature,
            "temperature_chamber": chamber,
        },
        failure_reason="temperature truth was not bound to the in-chamber platinum thermometer",
    )

    flow = dict(active_evidence.get("flow_monitor") or {})
    check(
        "dewpoint_output_flow_is_process_monitor",
        flow.get("source") == "dewpoint_instrument_output.flow_lpm"
        and flow.get("role") == "process_monitor_only"
        and flow.get("used_in_concentration_fit_or_correction") is False
        and flow.get("claimed_as_traceable_flow_reference") is False,
        evidence_detail=flow,
        failure_reason="dewpoint output flow was promoted beyond a process-monitor role",
    )

    timebase = dict(active_evidence.get("timebase") or {})
    check(
        "static_timebase_scope_bounded",
        timebase.get("source") == "software_monotonic_clock_plus_analyzer_1hz_stream"
        and timebase.get("role") == "static_sampling_and_stability_window_only"
        and timebase.get("sample_rate_hz") == 1
        and timebase.get("average1") == 49
        and timebase.get("average2") == 49
        and timebase.get("dynamic_response_or_ec_acceptance_in_scope") is False
        and timebase.get("claimed_as_external_traceable_timebase") is False,
        evidence_detail=timebase,
        failure_reason="static timebase was expanded to dynamic or traceable-time acceptance",
    )

    strict_gate_passed = (
        active_evidence.get("strict_original_certificate_gate_passed") is True
    )
    check(
        "strict_original_certificate_layer_remains_separate",
        strict_gate_passed is False,
        evidence_detail={
            "strict_original_certificate_gate_passed": strict_gate_passed,
            "remaining_gaps": active_contract.get("strict_original_certificate_gaps"),
        },
        failure_reason="strict original-certificate gate was falsely promoted",
    )

    passed = all(item["passed"] for item in checks)
    warnings: list[dict[str, Any]] = []
    days_until_dewpoint_expiry = (dewpoint_due - assessment_date).days
    if 0 <= days_until_dewpoint_expiry <= 30:
        warnings.append(
            {
                "warning_id": "dewpoint_certificate_near_expiry",
                "severity": "P1",
                "days_until_expiry": days_until_dewpoint_expiry,
                "valid_until": dewpoint.get("valid_until"),
            }
        )

    verification_mode = str(verification.get("verification_mode") or "unknown")
    suite_contract_self_test = verification_mode == "locked_fixture_contract_self_test"
    return {
        "artifact_type": "certificate_operational_admission",
        "artifact_role": "diagnostic_analysis",
        "schema_version": "certificate_operational_admission_v1",
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "contract_id": active_contract.get("contract_id"),
        "evidence_id": active_evidence.get("evidence_id"),
        "scope": active_contract.get("scope"),
        "evidence_source": (
            "simulated"
            if suite_contract_self_test
            else "owner_attested_local_documentary_evidence"
        ),
        "evidence_state": (
            "locked_fixture_contract_self_test"
            if suite_contract_self_test
            else "local_source_sha256_verified"
        ),
        "suite_contract_self_test": suite_contract_self_test,
        "status": (
            "PASSED_WITH_OWNER_ATTESTATION"
            if passed
            else "BLOCKED_OPERATIONAL_CERTIFICATE_GATE"
        ),
        "operational_certificate_gate_passed": passed,
        "offline_program_progress_allowed": passed,
        "strict_original_certificate_gate_passed": False,
        "formal_certificate_dossier_complete": False,
        "ready_for_real_execution": False,
        "execution_authorization_status": "not_requested_and_not_granted",
        "real_acceptance_status": "not_evaluated",
        "not_real_acceptance_evidence": True,
        "promotion_state": "blocked",
        "device_io_status": "not_attempted",
        "database_write_status": "not_attempted",
        "coefficient_fit_status": "not_attempted",
        "coefficient_writeback_status": "not_attempted",
        "real_primary_latest_refresh_status": "not_attempted",
        "source_mutation_status": "not_attempted",
        "source_verification": verification,
        "gate_checks": checks,
        "failed_gate_ids": [
            str(item["gate_id"]) for item in checks if not item["passed"]
        ],
        "warnings": warnings,
        "strict_original_certificate_gaps": list(
            active_contract.get("strict_original_certificate_gaps") or []
        ),
        "owner_attested_co2_values_ppm": observed_values,
        "physical_method": {
            "temperature_truth_source": temperature.get("truth_source"),
            "temperature_reference_placement": temperature.get("placement"),
            "flow_source": flow.get("source"),
            "flow_role": flow.get("role"),
            "timebase_source": timebase.get("source"),
            "co2_low_end_anchor_role": low_end.get("owner_accepted_role"),
            "h2o_low_end_anchor_role": separation.get("h2o_low_end_anchor_role"),
        },
        "interpretation": dict(active_contract.get("interpretation") or {}),
    }


def write_certificate_operational_admission_artifacts(
    result: Mapping[str, Any],
    *,
    output_dir: str | Path,
) -> dict[str, str]:
    target = Path(output_dir).resolve()
    target.mkdir(parents=True, exist_ok=True)

    rows_path = target / "certificate_operational_admission_execution_rows.csv"
    fieldnames = [
        "gate_id",
        "status",
        "passed",
        "severity",
        "evidence",
        "failure_reason",
    ]
    with rows_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for item in list(result.get("gate_checks") or []):
            writer.writerow(
                {
                    key: (
                        json.dumps(item.get(key), ensure_ascii=False)
                        if isinstance(item.get(key), (dict, list))
                        else item.get(key, "")
                    )
                    for key in fieldnames
                }
            )

    summary_payload = {
        "artifact_type": "certificate_operational_admission_summary",
        "artifact_role": "execution_summary",
        "schema_version": "certificate_operational_admission_summary_v1",
        "generated_at": result.get("generated_at"),
        "evidence_source": result.get("evidence_source"),
        "evidence_state": result.get("evidence_state"),
        "suite_contract_self_test": result.get("suite_contract_self_test"),
        "status": result.get("status"),
        "operational_certificate_gate_passed": result.get(
            "operational_certificate_gate_passed"
        ),
        "offline_program_progress_allowed": result.get(
            "offline_program_progress_allowed"
        ),
        "strict_original_certificate_gate_passed": False,
        "formal_certificate_dossier_complete": False,
        "ready_for_real_execution": False,
        "real_acceptance_status": "not_evaluated",
        "not_real_acceptance_evidence": True,
        "promotion_state": "blocked",
        "failed_gate_ids": result.get("failed_gate_ids"),
        "warnings": result.get("warnings"),
    }
    summary_path = _write_json(
        target / "certificate_operational_admission_execution_summary.json",
        summary_payload,
    )

    diagnostic_payload = dict(result)
    diagnostic_payload["artifact_role"] = "diagnostic_analysis"
    diagnostic_path = _write_json(
        target / "certificate_operational_admission_diagnostic_analysis.json",
        diagnostic_payload,
    )
    markdown_path = target / "certificate_operational_admission_diagnostic_analysis.md"
    markdown_path.write_text(
        _format_markdown(diagnostic_payload),
        encoding="utf-8",
    )

    formal_payload = {
        "artifact_type": "certificate_operational_admission_formal_analysis",
        "artifact_role": "formal_analysis",
        "schema_version": "certificate_operational_admission_formal_analysis_v1",
        "generated_at": result.get("generated_at"),
        "evidence_source": result.get("evidence_source"),
        "evidence_state": result.get("evidence_state"),
        "suite_contract_self_test": result.get("suite_contract_self_test"),
        "operational_certificate_gate_passed": result.get(
            "operational_certificate_gate_passed"
        ),
        "strict_original_certificate_gate_passed": False,
        "formal_certificate_dossier_complete": False,
        "ready_for_real_execution": False,
        "real_acceptance_status": "not_evaluated",
        "not_real_acceptance_evidence": True,
        "promotion_state": "blocked",
        "strict_original_certificate_gaps": result.get(
            "strict_original_certificate_gaps"
        ),
        "formal_conclusion": (
            "本工件只确认离线程序推进所需的运行资料门禁；"
            "不确认正式原始证书完整，不授权真机执行，也不是 real acceptance。"
        ),
    }
    formal_path = _write_json(
        target / "certificate_operational_admission_formal_analysis.json",
        formal_payload,
    )

    artifacts = {
        "execution_rows": str(rows_path),
        "execution_summary": str(summary_path),
        "diagnostic_analysis": str(diagnostic_path),
        "diagnostic_markdown": str(markdown_path),
        "formal_analysis": str(formal_path),
    }
    manifest_entries = []
    for role, raw_path in artifacts.items():
        path = Path(raw_path)
        manifest_entries.append(
            {
                "artifact_role": role,
                "filename": path.name,
                "sha256": sha256_file(path),
            }
        )
    manifest_path = _write_json(
        target / "certificate_operational_admission_sha256_manifest.json",
        {
            "artifact_type": "certificate_operational_admission_sha256_manifest",
            "artifact_role": "execution_rows",
            "schema_version": ("certificate_operational_admission_sha256_manifest_v1"),
            "not_real_acceptance_evidence": True,
            "entries": manifest_entries,
        },
    )
    artifacts["sha256_manifest"] = str(manifest_path)
    return artifacts


def _validate_runtime_payloads(
    contract: Mapping[str, Any],
    evidence: Mapping[str, Any],
) -> None:
    if dict(contract.get("evidence_boundary") or {}) != _BOUNDARY:
        raise ValueError("unsafe GA-D6B runtime contract boundary")
    if dict(evidence.get("execution_boundary") or {}) != _EXECUTION_BOUNDARY:
        raise ValueError("unsafe GA-D6B runtime evidence boundary")


def _format_markdown(result: Mapping[str, Any]) -> str:
    checks = list(result.get("gate_checks") or [])
    warnings = list(result.get("warnings") or [])
    lines = [
        "# GA-D6B 证书运行资料门禁",
        "",
        "> 本报告只允许离线程序、测试和治理继续推进；不连接真实 COM，"
        "不写设备、数据库或系数，不构成正式原始证书完整性或真机验收证据。",
        "",
        "## 结论",
        "",
        f"- 运行资料门禁：`{result.get('status')}`",
        (f"- 离线程序继续推进：`{result.get('offline_program_progress_allowed')}`"),
        "- 正式原始证书门禁：`False`",
        "- 真机执行授权：`False`",
        "- real acceptance：`not_evaluated`",
        "",
        "## 物理方法绑定",
        "",
        "- 温度真值：温箱内铂电阻数字测温仪；温箱设定值只作环境控制。",
        "- 流量：露点仪输出 `flow_lpm`，只作过程流量监测，不进入浓度拟合或修正。",
        "- 时间：软件单调时钟加分析仪 1 Hz 数据流，只用于静态采样和稳定窗口。",
        "- CO2 低端：干燥空气只作为业主认可的低端过程锚点，未宣称 CO2=0 ppm。",
        "- H2O 低端：必须由实际露点和当次实际压力确定，与 CO2 低端气保持独立。",
        "",
        "## 门禁检查",
        "",
        "| 门禁 | 状态 | 严重度 |",
        "|---|---|---|",
    ]
    for item in checks:
        lines.append(
            f"| `{item.get('gate_id')}` | `{item.get('status')}` | "
            f"`{item.get('severity')}` |"
        )
    lines.extend(["", "## 剩余正式证书缺口", ""])
    for item in list(result.get("strict_original_certificate_gaps") or []):
        lines.append(f"- `{item}`")
    lines.extend(["", "## 警告", ""])
    if not warnings:
        lines.append("- 无。")
    for item in warnings:
        lines.append(
            f"- `{item.get('warning_id')}`：剩余 "
            f"`{item.get('days_until_expiry')}` 天，到期日 "
            f"`{item.get('valid_until')}`。"
        )
    lines.append("")
    return "\n".join(lines)


__all__ = [
    "DEFAULT_CONTRACT_PATH",
    "DEFAULT_EVIDENCE_PATH",
    "build_locked_fixture_verification",
    "evaluate_certificate_operational_admission",
    "load_certificate_operational_admission_contract",
    "load_owner_attested_certificate_evidence",
    "verify_documentary_files",
    "write_certificate_operational_admission_artifacts",
]
