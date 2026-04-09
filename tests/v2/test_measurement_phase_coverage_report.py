from datetime import datetime, timedelta, timezone

from gas_calibrator.v2.core.controlled_state_machine_profile import build_state_transition_evidence
from gas_calibrator.v2.core.measurement_phase_coverage import (
    MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME,
    MEASUREMENT_PHASE_COVERAGE_REPORT_MARKDOWN_FILENAME,
    build_measurement_phase_coverage_report,
)
from gas_calibrator.v2.core.models import CalibrationPoint, SamplingResult
from gas_calibrator.v2.core.multi_source_stability import (
    MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME,
    MULTI_SOURCE_STABILITY_EVIDENCE_MARKDOWN_FILENAME,
    SIMULATION_EVIDENCE_SIDECAR_BUNDLE_FILENAME,
    build_multi_source_stability_evidence,
)


def _point(
    index: int,
    *,
    route: str,
    pressure_hpa: float | None = 1000.0,
    pressure_mode: str = "",
) -> CalibrationPoint:
    return CalibrationPoint(
        index=index,
        temperature_c=25.0,
        co2_ppm=400.0 if route != "h2o" else None,
        humidity_pct=45.0 if route == "h2o" else None,
        pressure_hpa=pressure_hpa,
        route=route,
        pressure_mode=pressure_mode,
        pressure_target_label="ambient" if pressure_mode == "ambient_open" else None,
        pressure_selection_token="ambient" if pressure_mode == "ambient_open" else "",
    )


def _sample(
    point: CalibrationPoint,
    *,
    seconds: int,
    point_phase: str,
    **overrides: float | str | bool | None,
) -> SamplingResult:
    payload = {
        "co2_ppm": 400.0 if point.route != "h2o" else None,
        "h2o_mmol": 0.72 if point.route == "h2o" else None,
        "co2_signal": 4500.0 if point.route != "h2o" else None,
        "h2o_signal": 2400.0 if point.route == "h2o" else None,
        "co2_ratio_f": 1.000 if point.route != "h2o" else None,
        "co2_ratio_raw": 1.001 if point.route != "h2o" else None,
        "h2o_ratio_f": 0.700 if point.route == "h2o" else None,
        "h2o_ratio_raw": 0.699 if point.route == "h2o" else None,
        "ref_signal": 3500.0,
        "temperature_c": 25.0,
        "pressure_hpa": None if point.pressure_mode == "ambient_open" else 1000.0,
        "dew_point_c": 5.2,
        "frame_has_data": True,
        "frame_usable": True,
        "point_phase": point_phase,
        "point_tag": point.pressure_target_label or point.route,
        "sample_index": max(1, seconds // 5 + 1),
        "stability_time_s": float(max(seconds, 1)),
        "total_time_s": 30.0,
    }
    payload.update(overrides)
    return SamplingResult(
        point=point,
        analyzer_id="ga01",
        timestamp=datetime(2026, 4, 9, 9, 0, tzinfo=timezone.utc) + timedelta(seconds=seconds),
        **payload,
    )


def test_measurement_phase_coverage_report_tracks_actual_model_test_and_gap() -> None:
    gas_point = _point(1, route="co2")
    samples = [
        _sample(gas_point, seconds=0, point_phase="sample_ready"),
        _sample(gas_point, seconds=12, point_phase="sample_ready", co2_ratio_raw=1.0004, co2_ppm=401.0),
    ]
    point_summaries = [
        {
            "point": {"index": 1, "route": "co2", "pressure_mode": "sealed"},
            "stats": {"point_phase": "sample_ready", "stability_time_s": 12.0},
        }
    ]
    stability = build_multi_source_stability_evidence(
        run_id="run_phase_coverage",
        samples=samples,
        point_summaries=point_summaries,
        artifact_paths={
            "multi_source_stability_evidence": MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME,
            "multi_source_stability_evidence_markdown": MULTI_SOURCE_STABILITY_EVIDENCE_MARKDOWN_FILENAME,
        },
    )
    transition = build_state_transition_evidence(
        run_id="run_phase_coverage",
        samples=samples,
        point_summaries=point_summaries,
    )

    report = build_measurement_phase_coverage_report(
        run_id="run_phase_coverage",
        samples=samples,
        point_summaries=point_summaries,
        multi_source_stability_evidence=stability,
        state_transition_evidence=transition,
        artifact_paths={
            "measurement_phase_coverage_report": MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME,
            "measurement_phase_coverage_report_markdown": MEASUREMENT_PHASE_COVERAGE_REPORT_MARKDOWN_FILENAME,
            "multi_source_stability_evidence": MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME,
            "state_transition_evidence": "state_transition_evidence.json",
            "simulation_evidence_sidecar_bundle": SIMULATION_EVIDENCE_SIDECAR_BUNDLE_FILENAME,
        },
        synthetic_trace_provenance={"summary": "synthetic simulation trace only"},
    )

    raw = report["raw"]
    rows_by_key = {
        str(row.get("phase_route_key") or ""): dict(row)
        for row in list(raw.get("phase_rows") or [])
    }

    assert report["artifact_type"] == "measurement_phase_coverage_report"
    assert report["filename"] == MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME
    assert report["markdown_filename"] == MEASUREMENT_PHASE_COVERAGE_REPORT_MARKDOWN_FILENAME
    assert raw["artifact_paths"]["measurement_phase_coverage_report"].endswith(
        MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME
    )
    assert raw["artifact_paths"]["measurement_phase_coverage_report_markdown"].endswith(
        MEASUREMENT_PHASE_COVERAGE_REPORT_MARKDOWN_FILENAME
    )
    assert rows_by_key["gas:sample_ready"]["evidence_source"] == "actual_simulated_run"
    assert rows_by_key["gas:preseal"]["evidence_source"] == "model_only"
    assert rows_by_key["water:preseal"]["evidence_source"] == "gap"
    assert rows_by_key["system:recovery_retry"]["evidence_source"] == "test_only"
    assert raw["review_surface"]["evidence_source_filters"] == [
        "gap",
        "model_only",
        "actual_simulated_run",
        "test_only",
    ]
    assert "Step 2 tail / Stage 3 bridge" in raw["digest"]["summary"]
    assert "shadow evaluation only" in raw["boundary_statements"]
    assert "does not modify live sampling gate by default" in raw["boundary_statements"]
    assert "not real acceptance" in report["markdown"]
    assert "compliance" not in raw
    assert "acceptance_level" not in raw


def test_measurement_phase_coverage_report_preserves_signal_group_gaps_honestly() -> None:
    ambient_point = _point(1, route="co2", pressure_hpa=None, pressure_mode="ambient_open")
    water_point = _point(2, route="h2o")
    samples = [
        _sample(
            ambient_point,
            seconds=0,
            point_phase="diagnostic",
            co2_signal=None,
            co2_ratio_f=None,
            co2_ratio_raw=None,
            pressure_hpa=1001.0,
        ),
        _sample(
            water_point,
            seconds=0,
            point_phase="preseal",
            co2_ppm=None,
            co2_ratio_f=None,
            co2_ratio_raw=None,
            co2_signal=None,
            h2o_signal=None,
        ),
    ]

    report = build_measurement_phase_coverage_report(
        run_id="run_phase_signal_gap",
        samples=samples,
        point_summaries=[
            {"point": {"index": 1, "route": "co2", "pressure_mode": "ambient_open"}, "stats": {"point_phase": "diagnostic"}},
            {"point": {"index": 2, "route": "h2o", "pressure_mode": "sealed"}, "stats": {"point_phase": "preseal"}},
        ],
        artifact_paths={
            "measurement_phase_coverage_report": MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME,
            "measurement_phase_coverage_report_markdown": MEASUREMENT_PHASE_COVERAGE_REPORT_MARKDOWN_FILENAME,
            "multi_source_stability_evidence": MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME,
            "state_transition_evidence": "state_transition_evidence.json",
            "simulation_evidence_sidecar_bundle": SIMULATION_EVIDENCE_SIDECAR_BUNDLE_FILENAME,
        },
    )

    rows_by_key = {
        str(row.get("phase_route_key") or ""): dict(row)
        for row in list(report["raw"].get("phase_rows") or [])
    }
    ambient_row = rows_by_key["ambient:ambient_diagnostic"]
    water_row = rows_by_key["water:preseal"]

    assert ambient_row["actual_run_evidence_present"] is True
    assert ambient_row["signal_group_coverage"]["reference"]["coverage_status"] == "complete"
    assert ambient_row["signal_group_coverage"]["analyzer_raw"]["available_channels"] == ["ref_signal"]
    assert ambient_row["signal_group_coverage"]["data_quality"]["coverage_status"] == "complete"
    assert water_row["signal_group_coverage"]["analyzer_raw"]["coverage_status"] == "partial"
    assert "h2o_signal" in water_row["missing_channels"]
    assert "h2o_ratio_raw" in water_row["available_channels"]
    assert "synthetic provenance" in "\n".join(report["raw"]["review_surface"]["detail_lines"])
