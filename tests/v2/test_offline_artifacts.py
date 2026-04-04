from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import numpy as np

from gas_calibrator.v2.core.models import CalibrationPoint, SamplingResult
from gas_calibrator.v2.core.offline_artifacts import (
    build_coefficient_registry,
    build_reference_quality_statistics,
    build_registry_indexes,
    build_run_analytics_summary,
    build_trend_registry,
    export_run_offline_artifacts,
)


def _sample(
    *,
    analyzer_id: str,
    usable: bool,
    has_reference: bool,
    thermometer_status: str = "",
    pressure_status: str = "",
) -> SamplingResult:
    point = CalibrationPoint(index=1, temperature_c=25.0, co2_ppm=400.0, pressure_hpa=1000.0, route="co2")
    return SamplingResult(
        point=point,
        analyzer_id=analyzer_id,
        timestamp=datetime(2026, 3, 26, 12, 0, 0, tzinfo=timezone.utc),
        co2_ppm=401.0,
        pressure_hpa=1000.0,
        pressure_gauge_hpa=998.0 if has_reference else None,
        pressure_reference_status=pressure_status,
        thermometer_temp_c=25.1 if has_reference else None,
        thermometer_reference_status=thermometer_status,
        frame_has_data=True,
        frame_usable=usable,
        frame_status="ok" if usable else "read_error",
    )


def test_run_analytics_summary_and_trend_registry_are_generated(tmp_path: Path) -> None:
    session = SimpleNamespace(config=SimpleNamespace(devices=SimpleNamespace(gas_analyzers=[SimpleNamespace(id="GA01", enabled=True), SimpleNamespace(id="GA02", enabled=True)])))
    samples = [_sample(analyzer_id="GA01", usable=True, has_reference=True), _sample(analyzer_id="GA02", usable=False, has_reference=False)]

    analytics = build_run_analytics_summary(
        run_id="run_test",
        run_dir=tmp_path,
        session=session,
        samples=samples,
        point_summaries=[
            {
                "point": {"index": 1, "route": "co2", "temperature_c": 25.0, "co2_ppm": 400.0},
                "stats": {"reason": "passed", "valid": True, "quality_score": 0.95},
            },
            {
                "point": {"index": 2, "route": "co2", "temperature_c": 25.0, "co2_ppm": 0.0},
                "stats": {
                    "reason": "outlier_ratio_too_high",
                    "valid": False,
                    "recommendation": "review",
                    "quality_score": 0.62,
                    "failed_checks": [{"rule_name": "signal_span", "message": "wide"}],
                },
            },
        ],
        export_statuses={"run_summary": {"status": "ok"}, "qc_report": {"status": "error"}},
    )
    trend = build_trend_registry(
        run_id="run_test",
        run_dir=tmp_path,
        output_dir=tmp_path,
        samples=samples,
        analytics_summary=analytics,
    )

    assert analytics["analyzer_coverage"]["coverage_text"] == "1/2"
    assert analytics["reference_quality_statistics"]["reference_quality"] == "degraded"
    assert analytics["export_resilience_status"]["overall_status"] == "degraded"
    assert analytics["qc_overview"]["decision_counts"]["warn"] == 1
    assert analytics["qc_overview"]["run_gate"]["status"] == "warn"
    assert analytics["qc_overview"]["failed_check_taxonomy"][0]["code"] == "signal_span"
    assert analytics["qc_overview"]["route_decision_breakdown"]["co2"]["warn"] == 1
    assert analytics["qc_reviewer_card"]["lines"]
    assert analytics["qc_evidence_section"]["cards"]
    assert analytics["qc_evidence_section"]["review_card_lines"]
    assert any(card["id"] == "boundary" for card in analytics["qc_review_cards"])
    assert analytics["unified_review_summary"]["qc_summary"]["summary"]
    assert any("运行门禁" in line for line in analytics["unified_review_summary"]["qc_summary"]["lines"])
    assert analytics["unified_review_summary"]["analytics_summary"]["summary"]
    assert any("漂移趋势" in line for line in analytics["unified_review_summary"]["analytics_summary"]["lines"])
    assert analytics["unified_review_summary"]["boundary_note"].startswith("证据边界:")
    assert analytics["unified_review_summary"]["reviewer_sections"][0]["id"] == "qc"
    assert any("质控" in line for line in analytics["unified_review_summary"]["reviewer_notes"])
    assert trend["analyzers"][0]["drift_indicator"] == "insufficient_history"
    assert trend["analyzers"][0]["spc_metric"]["metric"] == "usable_frame_ratio"
    assert trend["route_temp_source_groups"][0]["control_limit_status"] == "in_control"


def test_registry_indexes_and_coefficient_registry_capture_lineage_dimensions(tmp_path: Path) -> None:
    coeff_path = tmp_path / "calibration_coefficients.xlsx"
    coeff_path.write_text("", encoding="utf-8")
    samples = [_sample(analyzer_id="GA01", usable=True, has_reference=True)]
    registry = build_coefficient_registry(
        run_id="run_test",
        run_dir=tmp_path,
        samples=samples,
        export_statuses={"coefficient_report": {"status": "ok", "path": str(coeff_path)}},
        versions={"config_version": "cfg-a", "points_version": "pts-b", "profile_version": "2.5", "software_build_id": "build-1"},
        acceptance_plan={"not_real_acceptance_evidence": True},
        analytics_summary={"summary_parity_status": "MATCH", "reference_quality_statistics": {"reference_quality": "healthy"}},
    )
    indexes = build_registry_indexes(
        [
            {
                "artifact_id": "run_test:coefficient_registry",
                "evidence_source": "simulated",
                "evidence_state": "collected",
                "config_version": "cfg-a",
                "points_version": "pts-b",
                "profile_version": "2.5",
                "software_build_id": "build-1",
                "dimensions": {"suite": "nightly", "scenario": "summary_parity", "analyzer": ["GA01"], "route": ["co2"], "temp": [25.0]},
            }
        ]
    )

    assert registry["entries"][0]["source_artifact_ids"] == ["run_test:coefficient_report", "run_test:run_summary", "run_test:results_json"]
    assert registry["entries"][0]["not_real_acceptance_evidence"] is True
    assert indexes["by_evidence_source"]["simulated_protocol"] == ["run_test:coefficient_registry"]
    assert indexes["by_suite"]["nightly"] == ["run_test:coefficient_registry"]
    assert indexes["by_analyzer"]["GA01"] == ["run_test:coefficient_registry"]
    assert indexes["by_points_version"]["pts-b"] == ["run_test:coefficient_registry"]
    assert indexes["by_software_build_id"]["build-1"] == ["run_test:coefficient_registry"]


def test_reference_quality_statistics_report_healthy_and_missing() -> None:
    assert build_reference_quality_statistics(
        [_sample(analyzer_id="GA01", usable=True, has_reference=True, thermometer_status="healthy", pressure_status="healthy")]
    )["reference_quality"] == "healthy"
    assert build_reference_quality_statistics([_sample(analyzer_id="GA01", usable=True, has_reference=False)])["reference_quality"] == "missing"


def test_reference_quality_statistics_capture_status_driven_degradation() -> None:
    payload = build_reference_quality_statistics(
        [
            _sample(
                analyzer_id="GA01",
                usable=True,
                has_reference=True,
                thermometer_status="healthy",
                pressure_status="wrong_unit_configuration",
            )
        ]
    )

    assert payload["reference_quality"] == "degraded"
    assert payload["pressure_reference_status"] == "wrong_unit_configuration"
    assert payload["pressure_status_counts"]["wrong_unit_configuration"] == 1


def test_export_run_offline_artifacts_keeps_spectral_sidecar_disabled_by_default(tmp_path: Path) -> None:
    session = SimpleNamespace(
        config=SimpleNamespace(
            devices=SimpleNamespace(gas_analyzers=[SimpleNamespace(id="GA01", enabled=True)]),
            features=SimpleNamespace(simulation_mode=True, enable_spectral_quality_analysis=False),
            workflow=SimpleNamespace(profile_name="offline", profile_version="v2"),
        )
    )
    samples = [_sample(analyzer_id="GA01", usable=True, has_reference=True) for _ in range(4)]

    payload = export_run_offline_artifacts(
        run_dir=tmp_path,
        output_dir=tmp_path,
        run_id="run_test",
        session=session,
        samples=samples,
        point_summaries=[{"stats": {"reason": "passed"}}],
        output_files=[],
        export_statuses={"run_summary": {"status": "ok", "role": "execution_summary", "path": str(tmp_path / "summary.json")}},
        source_points_file=None,
        software_build_id="build-1",
    )

    assert "spectral_quality_summary" not in payload["artifact_statuses"]
    assert "spectral_quality_summary" not in payload["summary_stats"]
    assert not (tmp_path / "spectral_quality_summary.json").exists()


def test_export_run_offline_artifacts_writes_spectral_quality_summary_when_enabled(tmp_path: Path) -> None:
    session = SimpleNamespace(
        config=SimpleNamespace(
            devices=SimpleNamespace(gas_analyzers=[SimpleNamespace(id="GA01", enabled=True)]),
            features=SimpleNamespace(
                simulation_mode=True,
                enable_spectral_quality_analysis=True,
                spectral_min_samples=32,
                spectral_min_duration_s=20.0,
                spectral_low_freq_max_hz=0.05,
            ),
            workflow=SimpleNamespace(profile_name="offline", profile_version="v2"),
        )
    )
    samples = []
    for index in range(80):
        samples.append(
            replace(
                _sample(analyzer_id="GA01", usable=True, has_reference=True),
                timestamp=datetime(2026, 3, 26, 12, 0, 0, tzinfo=timezone.utc) + timedelta(seconds=index),
                co2_signal=float(np.sin(2.0 * np.pi * 0.1 * index)),
            )
        )

    payload = export_run_offline_artifacts(
        run_dir=tmp_path,
        output_dir=tmp_path,
        run_id="run_test",
        session=session,
        samples=samples,
        point_summaries=[{"stats": {"reason": "passed"}}],
        output_files=[],
        export_statuses={"run_summary": {"status": "ok", "role": "execution_summary", "path": str(tmp_path / "summary.json")}},
        source_points_file=None,
        software_build_id="build-1",
    )

    spectral_path = tmp_path / "spectral_quality_summary.json"
    assert spectral_path.exists()
    assert payload["artifact_statuses"]["spectral_quality_summary"]["role"] == "diagnostic_analysis"
    assert payload["summary_stats"]["spectral_quality_summary"]["artifact_type"] == "spectral_quality_summary"
    assert payload["summary_stats"]["spectral_quality_summary"]["not_real_acceptance_evidence"] is True
    assert payload["manifest_sections"]["spectral_quality"]["channel_count"] >= 1
    assert str(spectral_path) in list(payload["remembered_files"] or [])


def test_export_run_offline_artifacts_normalizes_skipped_spectral_summary_evidence_source(tmp_path: Path) -> None:
    session = SimpleNamespace(
        config=SimpleNamespace(
            devices=SimpleNamespace(gas_analyzers=[SimpleNamespace(id="GA01", enabled=True)]),
            features=SimpleNamespace(
                simulation_mode=True,
                enable_spectral_quality_analysis=True,
                spectral_min_samples=32,
                spectral_min_duration_s=20.0,
                spectral_low_freq_max_hz=0.05,
            ),
            workflow=SimpleNamespace(profile_name="offline", profile_version="v2"),
        )
    )

    payload = export_run_offline_artifacts(
        run_dir=tmp_path,
        output_dir=tmp_path,
        run_id="run_test",
        session=session,
        samples=[],
        point_summaries=[{"stats": {"reason": "passed"}}],
        output_files=[],
        export_statuses={"run_summary": {"status": "ok", "role": "execution_summary", "path": str(tmp_path / "summary.json")}},
        source_points_file=None,
        software_build_id="build-1",
    )

    assert payload["summary_stats"]["spectral_quality_summary"]["status"] == "insufficient_data"
    assert payload["summary_stats"]["spectral_quality_summary"]["evidence_source"] == "simulated_protocol"
