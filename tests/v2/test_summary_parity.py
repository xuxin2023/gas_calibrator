from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from gas_calibrator.logging_utils import RunLogger
from gas_calibrator.v2.core.models import CalibrationPoint, SamplingResult
from gas_calibrator.v2.export.ratio_poly_report import _point_integrity_text, build_analyzer_summary_frame


def _sample(
    *,
    analyzer_id: str,
    sample_index: int,
    frame_usable: bool,
    temperature_c: float = 20.0,
    pressure_hpa: float = 1000.0,
    pressure_gauge_hpa: float | None = None,
    thermometer_temp_c: float | None = None,
    dew_point_c: float | None = None,
    co2_ppm: float | None = 400.0,
    h2o_mmol: float | None = 6.0,
    co2_ratio_f: float | None = 1.2,
    h2o_ratio_f: float | None = 0.2,
    ref_signal: float | None = 2000.0,
    co2_signal: float | None = 1100.0,
    h2o_signal: float | None = 600.0,
    analyzer_pressure_kpa: float | None = None,
    analyzer_chamber_temp_c: float | None = None,
    case_temp_c: float | None = None,
) -> SamplingResult:
    point = CalibrationPoint(
        index=1,
        temperature_c=temperature_c,
        co2_ppm=400.0,
        pressure_hpa=1000.0,
        route="co2",
    )
    return SamplingResult(
        point=point,
        analyzer_id=analyzer_id,
        timestamp=datetime(2026, 3, 25, 12, 0, sample_index, tzinfo=timezone.utc),
        co2_ppm=co2_ppm,
        h2o_mmol=h2o_mmol,
        co2_signal=co2_signal,
        h2o_signal=h2o_signal,
        co2_ratio_f=co2_ratio_f,
        h2o_ratio_f=h2o_ratio_f,
        ref_signal=ref_signal,
        temperature_c=temperature_c,
        pressure_hpa=pressure_hpa,
        pressure_gauge_hpa=pressure_gauge_hpa,
        thermometer_temp_c=thermometer_temp_c,
        dew_point_c=dew_point_c,
        analyzer_pressure_kpa=analyzer_pressure_kpa,
        analyzer_chamber_temp_c=analyzer_chamber_temp_c,
        case_temp_c=case_temp_c,
        frame_has_data=True,
        frame_usable=frame_usable,
        frame_status="ok" if frame_usable else "read_error",
        point_phase="co2",
        point_tag="co2_1",
        sample_index=sample_index,
    )


def _build_v1_rows(samples: list[SamplingResult], *, expected_analyzers: list[str]) -> list[dict[str, object]]:
    point_present = {str(sample.analyzer_id).strip().upper() for sample in samples if str(sample.analyzer_id).strip()}
    point_usable = {
        str(sample.analyzer_id).strip().upper()
        for sample in samples
        if str(sample.analyzer_id).strip() and bool(sample.frame_usable)
    }
    expected_ids = [str(item).strip().upper() for item in expected_analyzers]
    missing_analyzers = sorted(set(expected_ids) - point_present)
    unusable_analyzers = sorted(point_present - point_usable)
    integrity = _point_integrity_text(expected_count=len(expected_ids), present=point_present, usable=point_usable)

    rows_by_index: dict[int, list[SamplingResult]] = {}
    for sample in samples:
        rows_by_index.setdefault(int(sample.sample_index or 0), []).append(sample)

    rows: list[dict[str, object]] = []
    for sample_index in sorted(rows_by_index):
        batch = rows_by_index[sample_index]
        first = batch[0]
        row: dict[str, object] = {
            "point_row": first.point.index,
            "point_phase": first.point.route,
            "point_tag": first.point_tag or "co2_1",
            "point_title": "co2 summary parity point",
            "temp_chamber_c": first.point.temperature_c,
            "hgen_temp_c": first.point.hgen_temp_c,
            "hgen_rh_pct": first.point.hgen_rh_pct,
            "co2_ppm_target": first.point.co2_ppm,
            "pressure_target_hpa": first.point.target_pressure_hpa,
            "sample_ts": first.timestamp.isoformat(),
            "dewpoint_c": next((sample.dew_point_c for sample in batch if sample.dew_point_c is not None), None),
            "pressure_hpa": next((sample.pressure_hpa for sample in batch if sample.pressure_hpa is not None), None),
            "pressure_gauge_hpa": next(
                (sample.pressure_gauge_hpa for sample in batch if sample.pressure_gauge_hpa is not None),
                None,
            ),
            "thermometer_temp_c": next(
                (sample.thermometer_temp_c for sample in batch if sample.thermometer_temp_c is not None),
                None,
            ),
            "analyzer_coverage_text": f"{len(point_usable)}/{len(expected_ids)}",
            "analyzer_usable_count": len(point_usable),
            "analyzer_expected_count": len(expected_ids),
            "analyzer_integrity": integrity,
            "analyzer_missing_labels": ",".join(missing_analyzers),
            "analyzer_unusable_labels": ",".join(unusable_analyzers),
        }
        for sample in batch:
            prefix = str(sample.analyzer_id).strip().lower()
            row[f"{prefix}_frame_has_data"] = bool(sample.frame_has_data)
            row[f"{prefix}_frame_usable"] = bool(sample.frame_usable)
            row[f"{prefix}_frame_status"] = str(sample.frame_status)
            row[f"{prefix}_co2_ppm"] = sample.co2_ppm
            row[f"{prefix}_h2o_mmol"] = sample.h2o_mmol
            row[f"{prefix}_co2_ratio_f"] = sample.co2_ratio_f
            row[f"{prefix}_h2o_ratio_f"] = sample.h2o_ratio_f
            row[f"{prefix}_ref_signal"] = sample.ref_signal
            row[f"{prefix}_co2_signal"] = sample.co2_signal
            row[f"{prefix}_h2o_signal"] = sample.h2o_signal
            row[f"{prefix}_pressure_kpa"] = sample.analyzer_pressure_kpa
            row[f"{prefix}_chamber_temp_c"] = sample.analyzer_chamber_temp_c
            row[f"{prefix}_case_temp_c"] = sample.case_temp_c
        rows.append(row)
    return rows


def _assert_summary_subset_matches(v1_row: dict[str, object], v2_row: dict[str, object]) -> None:
    float_fields = ("ppm_CO2", "ppm_H2O", "Temp", "P", "ppm_H2O_Dew")
    exact_fields = (
        "AnalyzerCoverage",
        "UsableAnalyzers",
        "ExpectedAnalyzers",
        "PointIntegrity",
        "MissingAnalyzers",
        "UnusableAnalyzers",
        "ValidFrames",
        "TotalFrames",
        "FrameStatus",
    )
    for field in float_fields:
        assert v1_row[field] == pytest.approx(v2_row[field], abs=1e-6), field
    for field in exact_fields:
        assert v1_row[field] == v2_row[field], field


def test_v1_v2_summary_parity_on_aligned_rows_uses_same_reference_subset(tmp_path: Path) -> None:
    samples = [
        _sample(
            analyzer_id="GA01",
            sample_index=1,
            frame_usable=True,
            pressure_hpa=1000.0,
            pressure_gauge_hpa=998.0,
            thermometer_temp_c=25.0,
            dew_point_c=2.0,
            co2_ppm=400.0,
            h2o_mmol=6.0,
            co2_ratio_f=1.2,
            h2o_ratio_f=0.2,
            analyzer_pressure_kpa=99.8,
            analyzer_chamber_temp_c=20.2,
            case_temp_c=20.8,
        ),
        _sample(
            analyzer_id="GA02",
            sample_index=1,
            frame_usable=False,
            pressure_hpa=1000.0,
            pressure_gauge_hpa=998.0,
            thermometer_temp_c=25.0,
            dew_point_c=2.0,
            co2_ppm=410.0,
            h2o_mmol=6.2,
            analyzer_pressure_kpa=99.8,
            analyzer_chamber_temp_c=20.3,
            case_temp_c=20.9,
        ),
        _sample(
            analyzer_id="GA01",
            sample_index=2,
            frame_usable=False,
            pressure_hpa=950.0,
            pressure_gauge_hpa=930.0,
            thermometer_temp_c=35.0,
            dew_point_c=8.0,
            co2_ppm=430.0,
            h2o_mmol=8.0,
            co2_ratio_f=1.5,
            h2o_ratio_f=0.4,
            analyzer_pressure_kpa=93.0,
            analyzer_chamber_temp_c=24.0,
            case_temp_c=24.8,
        ),
    ]
    expected_analyzers = ["GA01", "GA02", "GA03"]
    logger = RunLogger(tmp_path, run_id="summary_parity", cfg={"workflow": {"summary_alignment": {"reference_on_aligned_rows": True}}})
    try:
        v1_rows = _build_v1_rows(samples, expected_analyzers=expected_analyzers)
        v1_summary = logger._build_analyzer_summary_row(v1_rows, label="GA01", num=1)
    finally:
        logger.close()

    v2_frame = build_analyzer_summary_frame(
        samples,
        expected_analyzers=expected_analyzers,
        reference_on_aligned_rows=True,
    )
    v2_row = dict(v2_frame[v2_frame["Analyzer"] == "GA01"].iloc[0].to_dict())

    _assert_summary_subset_matches(v1_summary, v2_row)
    assert v1_summary["Temp"] == pytest.approx(25.0, abs=1e-6)
    assert v2_row["Temp"] == pytest.approx(25.0, abs=1e-6)
    assert v1_summary["P"] == pytest.approx(998.0, abs=1e-6)
    assert v2_row["P"] == pytest.approx(998.0, abs=1e-6)


def test_v1_v2_summary_parity_changes_when_reference_on_aligned_rows_is_disabled(tmp_path: Path) -> None:
    samples = [
        _sample(
            analyzer_id="GA01",
            sample_index=1,
            frame_usable=True,
            pressure_hpa=1000.0,
            pressure_gauge_hpa=998.0,
            thermometer_temp_c=25.0,
            dew_point_c=2.0,
            analyzer_pressure_kpa=99.8,
            analyzer_chamber_temp_c=20.2,
            case_temp_c=20.8,
        ),
        _sample(
            analyzer_id="GA01",
            sample_index=2,
            frame_usable=False,
            pressure_hpa=950.0,
            pressure_gauge_hpa=930.0,
            thermometer_temp_c=35.0,
            dew_point_c=8.0,
            analyzer_pressure_kpa=93.0,
            analyzer_chamber_temp_c=24.0,
            case_temp_c=24.8,
        ),
    ]
    logger = RunLogger(tmp_path, run_id="summary_unaligned", cfg={"workflow": {"summary_alignment": {"reference_on_aligned_rows": False}}})
    try:
        v1_rows = _build_v1_rows(samples, expected_analyzers=["GA01"])
        v1_summary = logger._build_analyzer_summary_row(v1_rows, label="GA01", num=1)
    finally:
        logger.close()

    v2_frame = build_analyzer_summary_frame(
        samples,
        expected_analyzers=["GA01"],
        reference_on_aligned_rows=False,
    )
    v2_row = dict(v2_frame.iloc[0].to_dict())

    _assert_summary_subset_matches(v1_summary, v2_row)
    assert v1_summary["Temp"] == pytest.approx(25.0, abs=1e-6)
    assert v2_row["Temp"] == pytest.approx(25.0, abs=1e-6)
    assert v1_summary["P"] == pytest.approx(964.0, abs=1e-6)
    assert v2_row["P"] == pytest.approx(964.0, abs=1e-6)
