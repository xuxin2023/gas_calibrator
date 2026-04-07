from __future__ import annotations

import csv

import pytest

from gas_calibrator.validation.single_gas_pressure_curve import (
    build_pressure_curve_point_means,
    generate_pressure_curve_plots,
    summarize_pressure_curve_relationships,
    write_pressure_curve_point_means_csv,
)


def _row(
    timestamp: str,
    *,
    phase: str,
    gas_ppm: int,
    pressure_target_hpa: int | None,
    gauge_pressure_hpa: float,
    co2_ratio_raw: float,
    co2_ratio_f: float,
    co2_density: float,
    co2_ppm: float,
    chamber_temp_c: float,
    shell_temp_c: float,
) -> dict[str, object]:
    return {
        "timestamp": timestamp,
        "process_variant": "B",
        "repeat_index": 1,
        "phase": phase,
        "gas_ppm": gas_ppm,
        "pressure_target_hpa": pressure_target_hpa,
        "gauge_pressure_hpa": gauge_pressure_hpa,
        "controller_pressure_hpa": gauge_pressure_hpa,
        "co2_ratio_raw": co2_ratio_raw,
        "co2_ratio_f": co2_ratio_f,
        "co2_density": co2_density,
        "co2_ppm": co2_ppm,
        "chamber_temp_c": chamber_temp_c,
        "shell_temp_c": shell_temp_c,
    }


def test_build_pressure_curve_point_means_and_exports(tmp_path) -> None:
    rows = [
        _row(
            "2026-04-07T10:00:00",
            phase="gas_flush_vent_on",
            gas_ppm=600,
            pressure_target_hpa=None,
            gauge_pressure_hpa=1003.0,
            co2_ratio_raw=1.1000,
            co2_ratio_f=1.0995,
            co2_density=0.4200,
            co2_ppm=601.0,
            chamber_temp_c=35.1,
            shell_temp_c=34.2,
        ),
        _row(
            "2026-04-07T10:00:40",
            phase="gas_flush_vent_on",
            gas_ppm=600,
            pressure_target_hpa=None,
            gauge_pressure_hpa=1002.0,
            co2_ratio_raw=1.1002,
            co2_ratio_f=1.0997,
            co2_density=0.4202,
            co2_ppm=602.0,
            chamber_temp_c=35.0,
            shell_temp_c=34.1,
        ),
        _row(
            "2026-04-07T10:01:10",
            phase="stable_sampling",
            gas_ppm=600,
            pressure_target_hpa=900,
            gauge_pressure_hpa=900.5,
            co2_ratio_raw=1.1020,
            co2_ratio_f=1.1015,
            co2_density=0.4010,
            co2_ppm=598.0,
            chamber_temp_c=35.4,
            shell_temp_c=34.5,
        ),
        _row(
            "2026-04-07T10:01:20",
            phase="stable_sampling",
            gas_ppm=600,
            pressure_target_hpa=900,
            gauge_pressure_hpa=899.5,
            co2_ratio_raw=1.1018,
            co2_ratio_f=1.1014,
            co2_density=0.4008,
            co2_ppm=597.0,
            chamber_temp_c=35.5,
            shell_temp_c=34.6,
        ),
        _row(
            "2026-04-07T10:02:10",
            phase="stable_sampling",
            gas_ppm=600,
            pressure_target_hpa=500,
            gauge_pressure_hpa=500.5,
            co2_ratio_raw=1.1040,
            co2_ratio_f=1.1033,
            co2_density=0.3210,
            co2_ppm=592.0,
            chamber_temp_c=35.8,
            shell_temp_c=34.8,
        ),
        _row(
            "2026-04-07T10:02:20",
            phase="stable_sampling",
            gas_ppm=600,
            pressure_target_hpa=500,
            gauge_pressure_hpa=499.5,
            co2_ratio_raw=1.1038,
            co2_ratio_f=1.1031,
            co2_density=0.3208,
            co2_ppm=591.0,
            chamber_temp_c=35.7,
            shell_temp_c=34.7,
        ),
    ]

    point_rows = build_pressure_curve_point_means(
        rows,
        process_variant="B",
        gas_ppm=600,
        repeat_index=1,
        ambient_tail_window_s=60.0,
    )

    assert [row["point_label"] for row in point_rows] == ["ambient", "900hPa", "500hPa"]
    assert point_rows[0]["co2_ratio_raw_mean"] == pytest.approx(1.1001)
    assert point_rows[0]["co2_ratio_f_mean"] == pytest.approx(1.0996)
    assert point_rows[0]["co2_density_mean"] == pytest.approx(0.4201)
    assert point_rows[0]["co2_ppm_mean"] == pytest.approx(601.5)
    assert point_rows[0]["pressure_hpa_mean"] == pytest.approx(1002.5)
    assert point_rows[0]["chamber_temp_mean"] == pytest.approx(35.05)
    assert point_rows[0]["shell_temp_mean"] == pytest.approx(34.15)
    assert point_rows[1]["pressure_hpa_mean"] == pytest.approx(900.0)
    assert point_rows[2]["pressure_hpa_mean"] == pytest.approx(500.0)
    assert point_rows[2]["density_over_pressure_mean"] == pytest.approx(0.3209 / 500.0)

    csv_path = write_pressure_curve_point_means_csv(tmp_path / "means.csv", point_rows)
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        exported = list(csv.DictReader(handle))
    assert len(exported) == 3
    assert exported[0]["co2_ratio_f_mean"] == "1.0996"
    assert exported[2]["point_label"] == "500hPa"

    summary = summarize_pressure_curve_relationships(point_rows)
    assert summary["point_count"] == 3
    assert summary["pressure_span_hpa"] == pytest.approx(502.5)

    plots = generate_pressure_curve_plots(tmp_path, point_rows=point_rows)
    assert set(plots.keys()) == {
        "ratio_vs_pressure",
        "density_vs_pressure",
        "ppm_vs_pressure",
        "ppm_vs_density_over_pressure",
    }
    for path in plots.values():
        assert path.exists()
