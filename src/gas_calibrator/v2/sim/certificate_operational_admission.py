"""Deterministic GA-D6B contract self-test for regression/nightly suites."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ..core.certificate_operational_admission import (
    build_locked_fixture_verification,
    evaluate_certificate_operational_admission,
    load_certificate_operational_admission_contract,
    load_owner_attested_certificate_evidence,
    write_certificate_operational_admission_artifacts,
)


def build_certificate_operational_admission_offline_report(
    *,
    report_root: str | Path,
    run_name: str = "ga_d6b_certificate_operational_admission",
) -> dict[str, Any]:
    contract = load_certificate_operational_admission_contract()
    evidence = load_owner_attested_certificate_evidence()
    verification = build_locked_fixture_verification(evidence)
    result = evaluate_certificate_operational_admission(
        evidence,
        contract=contract,
        source_verification=verification,
    )
    report_dir = Path(report_root).resolve() / run_name
    artifacts = write_certificate_operational_admission_artifacts(
        result,
        output_dir=report_dir,
    )
    contract_match = (
        result.get("status") == "PASSED_WITH_OWNER_ATTESTATION"
        and result.get("operational_certificate_gate_passed") is True
        and result.get("strict_original_certificate_gate_passed") is False
        and result.get("ready_for_real_execution") is False
    )
    return {
        "status": "MATCH" if contract_match else "MISMATCH",
        "compare_status": "MATCH" if contract_match else "MISMATCH",
        "report_dir": str(report_dir),
        "report_json": artifacts["diagnostic_analysis"],
        "report_markdown": artifacts["diagnostic_markdown"],
        "execution_rows": artifacts["execution_rows"],
        "execution_summary": artifacts["execution_summary"],
        "formal_analysis": artifacts["formal_analysis"],
        "report": result,
    }


__all__ = ["build_certificate_operational_admission_offline_report"]
