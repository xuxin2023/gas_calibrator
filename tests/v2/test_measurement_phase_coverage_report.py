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
    assert rows_by_key["gas:sample_ready"]["coverage_bucket"] == "actual_simulated_run_with_payload_complete"
    assert rows_by_key["gas:sample_ready"]["payload_completeness"] == "complete"
    assert rows_by_key["gas:sample_ready"]["available_signal_layers"] == [
        "reference",
        "analyzer_raw",
        "output",
        "data_quality",
    ]
    assert rows_by_key["gas:sample_ready"]["evidence_provenance"] == "actual_simulated_payload"
    assert rows_by_key["gas:preseal"]["coverage_bucket"] == "model_only"
    assert rows_by_key["gas:preseal"]["payload_completeness"] == "not_available"
    assert rows_by_key["water:preseal"]["coverage_bucket"] == "gap"
    assert rows_by_key["system:recovery_retry"]["coverage_bucket"] == "test_only"
    assert "actual_simulated_run_with_payload_complete" in raw["review_surface"]["evidence_source_filters"]
    assert "payload-complete" in raw["digest"]["summary"]
    assert raw["digest"]["payload_phase_summary"] == "gas/sample_ready"
    assert raw["digest"]["payload_complete_phase_summary"] == "gas/sample_ready"
    assert raw["digest"]["payload_partial_phase_summary"] == "no payload-partial simulated phase evidence"
    assert raw["digest"]["trace_only_phase_summary"] == "no trace-only phase buckets"
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
    assert ambient_row["coverage_bucket"] == "actual_simulated_run_with_payload_complete"
    assert ambient_row["payload_completeness"] == "complete"
    assert ambient_row["signal_group_coverage"]["reference"]["coverage_status"] == "complete"
    assert ambient_row["signal_group_coverage"]["analyzer_raw"]["available_channels"] == ["ref_signal"]
    assert ambient_row["signal_group_coverage"]["data_quality"]["coverage_status"] == "complete"
    assert ambient_row["evidence_provenance"] == "actual_simulated_payload"
    assert ambient_row["gap_classification"] == "ambient_baseline_payload_complete_anchor"
    assert ambient_row["gap_severity"] == "info"
    assert "Ambient baseline stabilization rule" in list(ambient_row.get("linked_method_confirmation_items") or [])
    assert "Ambient pressure baseline" in list(ambient_row.get("linked_uncertainty_inputs") or [])
    assert "Ambient environment reference chain" in list(ambient_row.get("linked_traceability_stub_nodes") or [])
    assert "synthetic baseline anchor" in str(ambient_row.get("reviewer_next_step_digest") or "")
    assert water_row["signal_group_coverage"]["analyzer_raw"]["coverage_status"] == "partial"
    assert water_row["coverage_bucket"] == "actual_simulated_run_with_payload_partial"
    assert water_row["payload_completeness"] == "partial"
    assert "h2o_signal" in water_row["missing_channels"]
    assert "h2o_ratio_raw" in water_row["available_channels"]
    assert water_row["missing_signal_layers"] == []
    assert any("payload stays partial" in str(item) for item in list(water_row["blockers"] or []))
    assert "synthetic provenance" in "\n".join(report["raw"]["review_surface"]["detail_lines"])


def test_measurement_phase_coverage_report_marks_trace_only_rich_profile_honestly() -> None:
    report = build_measurement_phase_coverage_report(
        run_id="run_trace_only_phase_coverage",
        samples=[],
        point_summaries=[],
        route_trace_events=[
            {
                "route": "ambient",
                "point_index": 1,
                "point_tag": "ambient_diagnostic_trace",
                "action": "ambient_diagnostic",
                "result": "simulation_only_synthetic",
                "message": "synthetic ambient diagnostic trace",
            },
            {
                "route": "ambient",
                "point_index": 1,
                "point_tag": "ambient_sample_ready_trace",
                "action": "ambient_sample_start",
                "result": "simulation_only_synthetic",
                "message": "synthetic ambient sample-ready trace",
            },
            {
                "route": "",
                "point_index": 0,
                "point_tag": "measurement_trace_recovery",
                "action": "retry_recovery",
                "result": "simulation_only_synthetic",
                "message": "synthetic recovery trace",
            },
        ],
        artifact_paths={
            "measurement_phase_coverage_report": MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME,
            "measurement_phase_coverage_report_markdown": MEASUREMENT_PHASE_COVERAGE_REPORT_MARKDOWN_FILENAME,
            "multi_source_stability_evidence": MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME,
            "state_transition_evidence": "state_transition_evidence.json",
            "simulation_evidence_sidecar_bundle": SIMULATION_EVIDENCE_SIDECAR_BUNDLE_FILENAME,
        },
        synthetic_trace_provenance={"summary": "measurement_trace_rich_v1 synthetic trace"},
    )

    rows_by_key = {
        str(row.get("phase_route_key") or ""): dict(row)
        for row in list(report["raw"].get("phase_rows") or [])
    }

    assert rows_by_key["ambient:ambient_diagnostic"]["coverage_bucket"] == "trace_only_not_evaluated"
    assert rows_by_key["ambient:sample_ready"]["coverage_bucket"] == "trace_only_not_evaluated"
    assert rows_by_key["system:recovery_retry"]["coverage_bucket"] == "trace_only_not_evaluated"
    assert rows_by_key["ambient:ambient_diagnostic"]["payload_completeness"] == "trace_only"
    assert rows_by_key["ambient:ambient_diagnostic"]["gap_classification"] == "ambient_baseline_trace_only_gap"
    assert rows_by_key["ambient:sample_ready"]["gap_classification"] == "ambient_sample_ready_trace_only_gap"
    assert rows_by_key["system:recovery_retry"]["gap_classification"] == "recovery_retry_trace_only_gap"
    assert rows_by_key["ambient:ambient_diagnostic"]["gap_severity"] == "medium"
    assert rows_by_key["system:recovery_retry"]["gap_severity"] == "medium"
    assert "Ambient baseline stabilization rule" in list(
        rows_by_key["ambient:ambient_diagnostic"].get("linked_method_confirmation_items") or []
    )
    assert "Ambient stabilization window" in list(
        rows_by_key["ambient:sample_ready"].get("linked_uncertainty_inputs") or []
    )
    assert "Software event log chain" in list(
        rows_by_key["system:recovery_retry"].get("linked_traceability_stub_nodes") or []
    )
    assert "trace into payload-backed reviewer evidence" in str(
        rows_by_key["ambient:ambient_diagnostic"].get("reviewer_next_step_digest") or ""
    )
    assert "software validation" in str(rows_by_key["system:recovery_retry"].get("reviewer_next_step_digest") or "").lower()
    assert rows_by_key["system:recovery_retry"]["evidence_provenance"] == "synthetic_trace_only"
    assert rows_by_key["ambient:ambient_diagnostic"]["signal_group_coverage"]["reference"]["coverage_status"] == "gap"
    assert rows_by_key["system:recovery_retry"]["missing_signal_layers"] == [
        "reference",
        "analyzer_raw",
        "output",
        "data_quality",
    ]
    assert "measurement_trace_rich_v1 synthetic trace" in "\n".join(report["raw"]["review_surface"]["detail_lines"])
    assert "trace-only" in report["raw"]["digest"]["summary"]


def test_measurement_phase_coverage_report_promotes_rich_phase_payloads_to_payload_backed_buckets() -> None:
    ambient_point = _point(10, route="co2", pressure_hpa=None, pressure_mode="ambient_open")
    recovery_point = _point(11, route="", pressure_hpa=1000.0, pressure_mode="")
    samples = [
        _sample(
            ambient_point,
            seconds=0,
            point_phase="ambient_diagnostic",
            point_tag="synthetic_ambient_diagnostic",
            frame_status="simulation_payload_synthetic",
            pressure_hpa=1001.4,
            co2_signal=None,
            co2_ratio_f=None,
            co2_ratio_raw=None,
        ),
        _sample(
            ambient_point,
            seconds=8,
            point_phase="sample_ready",
            point_tag="synthetic_ambient_sample_ready",
            frame_status="simulation_payload_synthetic",
            pressure_hpa=1001.2,
            co2_signal=None,
            co2_ratio_f=None,
            co2_ratio_raw=None,
        ),
        _sample(
            recovery_point,
            seconds=0,
            point_phase="recovery_retry",
            point_tag="synthetic_recovery_retry",
            frame_status="simulation_payload_synthetic_recovery",
            pressure_hpa=1000.1,
            co2_signal=None,
            co2_ratio_f=None,
            co2_ratio_raw=None,
            frame_usable=False,
        ),
        _sample(
            recovery_point,
            seconds=5,
            point_phase="recovery_retry",
            point_tag="synthetic_recovery_retry",
            frame_status="simulation_payload_synthetic_recovery",
            pressure_hpa=1000.0,
            co2_signal=None,
            co2_ratio_f=None,
            co2_ratio_raw=None,
        ),
    ]

    report = build_measurement_phase_coverage_report(
        run_id="run_payload_promotion",
        samples=samples,
        point_summaries=[],
        artifact_paths={
            "measurement_phase_coverage_report": MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME,
            "measurement_phase_coverage_report_markdown": MEASUREMENT_PHASE_COVERAGE_REPORT_MARKDOWN_FILENAME,
            "multi_source_stability_evidence": MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME,
            "state_transition_evidence": "state_transition_evidence.json",
            "simulation_evidence_sidecar_bundle": SIMULATION_EVIDENCE_SIDECAR_BUNDLE_FILENAME,
        },
        synthetic_trace_provenance={
            "summary": "measurement_trace_rich_v1 synthetic payload",
            "contains_synthetic_channel_injection": True,
        },
    )

    rows_by_key = {
        str(row.get("phase_route_key") or ""): dict(row)
        for row in list(report["raw"].get("phase_rows") or [])
    }

    assert rows_by_key["ambient:ambient_diagnostic"]["coverage_bucket"] == "actual_simulated_run_with_payload_complete"
    assert rows_by_key["ambient:sample_ready"]["coverage_bucket"] == "actual_simulated_run_with_payload_complete"
    assert rows_by_key["system:recovery_retry"]["coverage_bucket"] == "actual_simulated_run_with_payload_complete"
    assert rows_by_key["ambient:ambient_diagnostic"]["payload_completeness"] == "complete"
    assert rows_by_key["system:recovery_retry"]["payload_completeness"] == "complete"
    assert rows_by_key["ambient:ambient_diagnostic"]["gap_classification"] == "ambient_baseline_payload_complete_anchor"
    assert rows_by_key["ambient:sample_ready"]["gap_classification"] == "ambient_sample_ready_payload_complete_anchor"
    assert rows_by_key["system:recovery_retry"]["gap_classification"] == "recovery_retry_payload_complete_anchor"
    assert "Ambient baseline stabilization rule" in list(
        rows_by_key["ambient:ambient_diagnostic"].get("linked_method_confirmation_items") or []
    )
    assert "Ambient stabilization window" in list(
        rows_by_key["ambient:sample_ready"].get("linked_uncertainty_inputs") or []
    )
    assert "Software event log chain" in list(
        rows_by_key["system:recovery_retry"].get("linked_traceability_stub_nodes") or []
    )
    assert "synthetic release anchor" in str(rows_by_key["ambient:sample_ready"].get("reviewer_next_step_digest") or "")
    assert "software-validation anchor" in str(rows_by_key["system:recovery_retry"].get("reviewer_next_step_digest") or "")
    assert rows_by_key["system:recovery_retry"]["available_signal_layers"] == [
        "reference",
        "analyzer_raw",
        "output",
        "data_quality",
    ]
    assert rows_by_key["system:recovery_retry"]["missing_signal_layers"] == []
    assert rows_by_key["system:recovery_retry"]["evidence_provenance"] == "synthetic_sample_payload"
    assert "shadow evaluation only" in rows_by_key["system:recovery_retry"]["boundary_digest"]
    assert report["raw"]["digest"]["payload_phase_summary"] == (
        "ambient/ambient_diagnostic | ambient/sample_ready | system/recovery_retry"
    )
    assert report["raw"]["digest"]["payload_complete_phase_summary"] == (
        "ambient/ambient_diagnostic | ambient/sample_ready | system/recovery_retry"
    )
    assert report["raw"]["digest"]["payload_partial_phase_summary"] == "no payload-partial simulated phase evidence"
    assert "Ambient baseline stabilization rule" in str(report["raw"]["digest"]["linked_method_confirmation_summary"] or "")
    assert "Ambient stabilization window" in str(report["raw"]["digest"]["linked_uncertainty_input_summary"] or "")
    assert "Software event log chain" in str(report["raw"]["digest"]["linked_traceability_stub_summary"] or "")
    assert "payload-backed ambient/recovery phases" in str(report["raw"]["digest"]["phase_contrast_summary"] or "")


def test_measurement_phase_coverage_report_distinguishes_partial_vs_complete_richer_phase_links() -> None:
    water_point = _point(20, route="h2o")
    gas_point = _point(21, route="co2")
    samples = [
        _sample(
            water_point,
            seconds=0,
            point_phase="preseal",
            point_tag="synthetic_water_preseal_partial_payload",
            frame_status="simulation_payload_synthetic_preseal_partial",
            h2o_mmol=None,
            h2o_ratio_f=None,
        ),
        _sample(
            water_point,
            seconds=8,
            point_phase="preseal",
            point_tag="synthetic_water_preseal_partial_payload",
            frame_status="simulation_payload_synthetic_preseal_partial",
            h2o_mmol=None,
            h2o_ratio_f=None,
        ),
        _sample(
            gas_point,
            seconds=0,
            point_phase="pressure_stable",
            point_tag="synthetic_gas_pressure_stable_complete",
            frame_status="simulation_payload_synthetic_pressure_stable",
        ),
        _sample(
            gas_point,
            seconds=8,
            point_phase="pressure_stable",
            point_tag="synthetic_gas_pressure_stable_complete",
            frame_status="simulation_payload_synthetic_pressure_stable",
        ),
    ]

    report = build_measurement_phase_coverage_report(
        run_id="run_partial_complete_richer_phase_links",
        samples=samples,
        point_summaries=[],
        artifact_paths={
            "measurement_phase_coverage_report": MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME,
            "measurement_phase_coverage_report_markdown": MEASUREMENT_PHASE_COVERAGE_REPORT_MARKDOWN_FILENAME,
            "multi_source_stability_evidence": MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME,
            "state_transition_evidence": "state_transition_evidence.json",
            "simulation_evidence_sidecar_bundle": SIMULATION_EVIDENCE_SIDECAR_BUNDLE_FILENAME,
        },
        synthetic_trace_provenance={"summary": "measurement_trace_rich_v1 synthetic payload"},
    )

    rows_by_key = {
        str(row.get("phase_route_key") or ""): dict(row)
        for row in list(report["raw"].get("phase_rows") or [])
    }
    water_preseal_row = rows_by_key["water:preseal"]
    gas_pressure_row = rows_by_key["gas:pressure_stable"]

    assert water_preseal_row["coverage_bucket"] == "actual_simulated_run_with_payload_partial"
    assert water_preseal_row["payload_completeness"] == "partial"
    assert water_preseal_row["missing_signal_layers"] == ["output"]
    assert "conditioning window" in str(water_preseal_row.get("missing_reason_digest") or "")
    assert water_preseal_row["evidence_provenance"] == "synthetic_sample_payload"
    assert "honesty boundary" in str(water_preseal_row.get("phase_boundary_digest") or "")
    assert "scope / method / uncertainty" not in str(water_preseal_row.get("non_claim_digest") or "")
    assert "boundary" in str(water_preseal_row.get("reviewer_guidance_digest") or "")
    assert water_preseal_row["gap_classification"] == "conditioning_window_partial_payload"
    assert water_preseal_row["gap_severity"] == "high"
    assert "Water preseal window definition" in list(water_preseal_row.get("linked_method_confirmation_items") or [])
    assert "Humidity reference window" in list(water_preseal_row.get("linked_uncertainty_inputs") or [])
    assert "Humidity reference chain" in list(water_preseal_row.get("linked_traceability_stub_nodes") or [])
    assert "traceability stub" in str(water_preseal_row.get("reviewer_next_step_digest") or "")
    assert "scope_definition_pack" in list(water_preseal_row.get("next_required_artifacts") or [])
    assert "method_confirmation_matrix" in list(water_preseal_row.get("next_required_artifacts") or [])
    assert any(
        str(item.get("artifact_type") or "") == "uncertainty_budget_stub"
        for item in list(water_preseal_row.get("linked_readiness_artifact_refs") or [])
    )
    assert any(
        str(item.get("artifact_type") or "") == "metrology_traceability_stub"
        for item in list(water_preseal_row.get("linked_readiness_artifact_refs") or [])
    )
    assert "not live gate" in str(water_preseal_row.get("non_claim_digest") or "")

    assert gas_pressure_row["coverage_bucket"] == "actual_simulated_run_with_payload_complete"
    assert gas_pressure_row["payload_completeness"] == "complete"
    assert gas_pressure_row["missing_signal_layers"] == []
    assert "synthetic reviewer evidence only" in str(gas_pressure_row.get("phase_boundary_digest") or "")
    assert gas_pressure_row["gap_classification"] == "payload_complete_synthetic_reviewer_anchor"
    assert gas_pressure_row["gap_severity"] == "info"
    assert "Gas pressure stabilization hold confirmation" in list(gas_pressure_row.get("linked_method_confirmation_items") or [])
    assert "Reference gas value" in list(gas_pressure_row.get("linked_uncertainty_inputs") or [])
    assert "Standard gas chain" in list(gas_pressure_row.get("linked_traceability_stub_nodes") or [])
    assert any(
        str(item.get("artifact_type") or "") == "reference_asset_registry"
        for item in list(gas_pressure_row.get("linked_readiness_artifact_refs") or [])
    )
    assert any(
        str(item.get("artifact_type") or "") == "uncertainty_method_readiness_summary"
        for item in list(gas_pressure_row.get("linked_readiness_artifact_refs") or [])
    )

    assert report["raw"]["digest"]["payload_complete_phase_summary"] == "gas/pressure_stable"
    assert report["raw"]["digest"]["payload_partial_phase_summary"] == "water/preseal"
    assert "scope_definition_pack" in str(report["raw"]["digest"]["next_required_artifacts_summary"] or "")
    assert "Water preseal window definition" in str(report["raw"]["digest"]["linked_method_confirmation_summary"] or "")
    assert "Humidity reference window" in str(report["raw"]["digest"]["linked_uncertainty_input_summary"] or "")
    assert "Humidity reference chain" in str(report["raw"]["digest"]["linked_traceability_stub_summary"] or "")
    assert "conditioning_window_partial_payload / high" in str(report["raw"]["digest"]["gap_index_summary"] or "")
    assert "traceability stub" in str(report["raw"]["digest"]["reviewer_next_step_summary"] or "")
    assert "preseal stays payload-partial" in str(report["raw"]["digest"]["phase_contrast_summary"] or "")


def test_measurement_phase_coverage_report_tracks_gas_preseal_partial_gap_navigation() -> None:
    gas_point = _point(22, route="co2")
    water_point = _point(23, route="h2o")
    samples = [
        _sample(
            gas_point,
            seconds=0,
            point_phase="preseal",
            point_tag="synthetic_gas_preseal_partial_payload",
            frame_status="simulation_payload_synthetic_preseal_partial",
            co2_ppm=None,
            co2_ratio_f=None,
        ),
        _sample(
            gas_point,
            seconds=8,
            point_phase="preseal",
            point_tag="synthetic_gas_preseal_partial_payload",
            frame_status="simulation_payload_synthetic_preseal_partial",
            co2_ppm=None,
            co2_ratio_f=None,
        ),
        _sample(
            water_point,
            seconds=0,
            point_phase="pressure_stable",
            point_tag="synthetic_water_pressure_stable_complete",
            frame_status="simulation_payload_synthetic_pressure_stable",
        ),
        _sample(
            water_point,
            seconds=8,
            point_phase="pressure_stable",
            point_tag="synthetic_water_pressure_stable_complete",
            frame_status="simulation_payload_synthetic_pressure_stable",
        ),
    ]

    report = build_measurement_phase_coverage_report(
        run_id="run_gas_preseal_partial_gap_navigation",
        samples=samples,
        point_summaries=[],
        artifact_paths={
            "measurement_phase_coverage_report": MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME,
            "measurement_phase_coverage_report_markdown": MEASUREMENT_PHASE_COVERAGE_REPORT_MARKDOWN_FILENAME,
            "multi_source_stability_evidence": MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME,
            "state_transition_evidence": "state_transition_evidence.json",
            "simulation_evidence_sidecar_bundle": SIMULATION_EVIDENCE_SIDECAR_BUNDLE_FILENAME,
        },
        synthetic_trace_provenance={"summary": "measurement_trace_rich_v1 synthetic payload"},
    )

    rows_by_key = {
        str(row.get("phase_route_key") or ""): dict(row)
        for row in list(report["raw"].get("phase_rows") or [])
    }
    gas_preseal_row = rows_by_key["gas:preseal"]

    assert gas_preseal_row["coverage_bucket"] == "actual_simulated_run_with_payload_partial"
    assert gas_preseal_row["payload_completeness"] == "partial"
    assert gas_preseal_row["missing_signal_layers"] == ["output"]
    assert gas_preseal_row["gap_classification"] == "conditioning_window_partial_payload"
    assert gas_preseal_row["gap_severity"] == "high"
    assert "Gas preseal window definition" in list(gas_preseal_row.get("linked_method_confirmation_items") or [])
    assert "Reference gas window" in list(gas_preseal_row.get("linked_uncertainty_inputs") or [])
    assert "Standard gas chain" in list(gas_preseal_row.get("linked_traceability_stub_nodes") or [])
    assert "preseal partial" in str(gas_preseal_row.get("reviewer_next_step_digest") or "").lower()
    assert "analyzer_raw" not in list(gas_preseal_row.get("missing_signal_layers") or [])
    assert "released measurement output" in str(gas_preseal_row.get("phase_boundary_digest") or "")
