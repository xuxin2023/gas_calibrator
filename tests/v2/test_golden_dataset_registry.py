from __future__ import annotations

from gas_calibrator.v2.core.golden_dataset_registry import build_golden_dataset_registry


def test_golden_dataset_registry_covers_expected_axes() -> None:
    registry = build_golden_dataset_registry()
    summary = dict(registry.get("summary") or {})
    cases = {
        str(item.get("case_id") or ""): dict(item)
        for item in list(registry.get("cases") or [])
        if isinstance(item, dict)
    }

    assert registry["schema_version"] == "golden-dataset-registry-v1"
    assert registry["evidence_source"] == "simulated"
    assert registry["not_real_acceptance_evidence"] is True
    assert summary["gas_family_counts"]["co2"] >= 1
    assert summary["gas_family_counts"]["h2o"] >= 1
    assert summary["path_category_counts"]["gas_path"] >= 1
    assert summary["path_category_counts"]["water_path"] >= 1
    assert summary["path_category_counts"]["environment"] >= 1
    assert summary["temperature_point_category_counts"]["zero_or_cold"] >= 1
    assert summary["temperature_point_category_counts"]["ambient"] >= 1
    assert summary["pressure_point_category_counts"]["low_pressure"] >= 1
    assert summary["pressure_point_category_counts"]["high_pressure"] >= 1

    full_route = cases["full_route_success_all_temps_all_sources"]
    assert set(full_route["gas_families"]) == {"co2", "h2o"}
    assert set(full_route["temperature_point_categories"]) == {"ambient", "hot", "zero_or_cold"}
    assert set(full_route["pressure_point_categories"]) == {"high_pressure", "low_pressure", "nominal_pressure"}

    analyzer_fleet = cases["co2_only_skip0_success_eight_analyzers_with_relay"]
    assert analyzer_fleet["analyzer_population_category"] == "fleet_8"
    assert analyzer_fleet["chain_length_category"] in {"full_chain", "extended_chain"}

    humidity_timeout = cases["humidity_generator_timeout"]
    assert "humidity_timeout" in list(humidity_timeout.get("anomaly_scenarios") or [])
    assert "environment" in list(humidity_timeout.get("path_categories") or [])

    replay_only = cases["v1_route_trace_missing_but_io_log_derivable"]
    assert replay_only["source_kinds"] == ["replay_fixture"]
    assert list(replay_only.get("anomaly_scenarios") or [])
