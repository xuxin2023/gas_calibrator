from __future__ import annotations

import json
from pathlib import Path

import pytest

from gas_calibrator.v1_5.certificate_metrics_registry import (
    BOUNDARY,
    CertificateMetricsRegistry,
)


def _complete_record() -> dict[str, object]:
    return {
        "asset_id": "co2-cylinder-400ppm",
        "asset_name": "CO2 标准气体 400 ppm",
        "asset_type": "co2_standard_gas",
        "measurand": "CO2",
        "certificate_id": "CERT-CO2-400-2026",
        "nominal_value": "400",
        "certified_value": "399.67",
        "unit": "ppm",
        "expanded_uncertainty": "0.40",
        "coverage_factor": "2",
        "uncertainty_unit": "ppm",
        "valid_from": "2026-01-01",
        "valid_until": "2027-01-01",
        "traceability_chain": "NMI -> accredited laboratory -> cylinder",
        "evidence_file_path": r"D:\certificates\co2_400.pdf",
        "evidence_file_sha256": "a" * 64,
    }


def test_registry_saves_revisioned_drafts_without_crossing_calibration_boundary(
    tmp_path: Path,
) -> None:
    path = tmp_path / "certificate_metrics_registry.json"
    registry = CertificateMetricsRegistry(path)

    first = registry.save_record(_complete_record())
    second = registry.save_record(
        {
            **_complete_record(),
            "record_id": first["record_id"],
            "certified_value": "399.56",
        }
    )

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["boundary"] == BOUNDARY
    assert payload["boundary"]["calibration_input_connected"] is False
    assert payload["boundary"]["coefficient_write_allowed"] is False
    assert payload["boundary"]["device_io_allowed"] is False
    assert second["revision"] == 2
    assert second["certified_value"] == pytest.approx(399.56)
    assert len(second["revision_history"]) == 1
    assert second["revision_history"][0]["certified_value"] == pytest.approx(399.67)
    assert len(payload["audit_events"]) == 2
    assert all(
        event["device_io_performed"] is False for event in payload["audit_events"]
    )
    assert list(tmp_path.glob("*.tmp")) == []


def test_submit_for_review_requires_traceable_certificate_fields(
    tmp_path: Path,
) -> None:
    registry = CertificateMetricsRegistry(
        tmp_path / "certificate_metrics_registry.json"
    )

    with pytest.raises(ValueError, match="提交复核前仍缺少"):
        registry.save_record(
            {
                "asset_id": "co2-cylinder-100ppm",
                "asset_name": "CO2 标准气体 100 ppm",
            },
            submit_for_review=True,
        )

    submitted = registry.save_record(_complete_record(), submit_for_review=True)
    assert submitted["review_state"] == "pending_review"
    assert submitted["calibration_input_connected"] is False


def test_registry_rejects_invalid_dates_and_uncertainty(tmp_path: Path) -> None:
    registry = CertificateMetricsRegistry(
        tmp_path / "certificate_metrics_registry.json"
    )

    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        registry.save_record({**_complete_record(), "valid_until": "2027/01/01"})
    with pytest.raises(ValueError, match="不能小于 0"):
        registry.save_record({**_complete_record(), "expanded_uncertainty": "-0.1"})
    with pytest.raises(ValueError, match="截止日不能早于开始日"):
        registry.save_record(
            {
                **_complete_record(),
                "valid_from": "2027-02-01",
                "valid_until": "2027-01-01",
            }
        )


def test_registry_rejects_boundary_drift(tmp_path: Path) -> None:
    path = tmp_path / "certificate_metrics_registry.json"
    payload = {
        "schema_version": "certificate_metrics_registry_v1",
        "boundary": {**BOUNDARY, "calibration_input_connected": True},
        "records": [],
        "audit_events": [],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="安全边界"):
        CertificateMetricsRegistry(path).load()
