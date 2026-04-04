from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from gas_calibrator.logging_utils import RunLogger as V1RunLogger

from ..core.models import CalibrationPoint, SamplingResult
from ..export.ratio_poly_report import _point_integrity_text, build_analyzer_summary_frame
from ..ui_v2.i18n import (
    display_acceptance_value,
    display_bool,
    display_compare_status,
    display_evidence_source,
    display_risk_level,
    t,
)

PARITY_FLOAT_FIELDS = ("ppm_CO2", "ppm_H2O", "Temp", "P", "ppm_H2O_Dew")
PARITY_FLOAT_TOLERANCE = 1e-6
PARITY_EXACT_FIELDS = (
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
PARITY_TOLERANCE_RULES = {
    "default_float_abs": PARITY_FLOAT_TOLERANCE,
    "float_fields": list(PARITY_FLOAT_FIELDS),
    "exact_fields": list(PARITY_EXACT_FIELDS),
}


@dataclass(frozen=True)
class SummaryParityCase:
    name: str
    reference_on_aligned_rows: bool
    expected_analyzers: list[str]
    samples: list[SamplingResult]


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
            "point_title": "summary parity point",
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


def _parity_cases() -> list[SummaryParityCase]:
    return [
        SummaryParityCase(
            name="reference_on_aligned_rows",
            reference_on_aligned_rows=True,
            expected_analyzers=["GA01", "GA02", "GA03"],
            samples=[
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
            ],
        ),
        SummaryParityCase(
            name="reference_pool_pressure_expansion",
            reference_on_aligned_rows=False,
            expected_analyzers=["GA01"],
            samples=[
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
            ],
        ),
    ]


def build_summary_parity_report(*, report_root: Path, run_name: Optional[str] = None) -> dict[str, Any]:
    report_dir = Path(report_root) / str(run_name or "summary_parity")
    report_dir.mkdir(parents=True, exist_ok=True)
    cases_payload: list[dict[str, Any]] = []
    overall_match = True
    for case in _parity_cases():
        logger = V1RunLogger(
            report_dir / case.name,
            run_id=case.name,
            cfg={"workflow": {"summary_alignment": {"reference_on_aligned_rows": case.reference_on_aligned_rows}}},
        )
        try:
            v1_rows = _build_v1_rows(case.samples, expected_analyzers=case.expected_analyzers)
            v1_row = logger._build_analyzer_summary_row(v1_rows, label="GA01", num=1)
        finally:
            logger.close()
        v2_frame = build_analyzer_summary_frame(
            case.samples,
            expected_analyzers=case.expected_analyzers,
            reference_on_aligned_rows=case.reference_on_aligned_rows,
        )
        v2_row = dict(v2_frame[v2_frame["Analyzer"] == "GA01"].iloc[0].to_dict())
        comparisons: list[dict[str, Any]] = []
        case_match = True
        for field in PARITY_FLOAT_FIELDS:
            v1_value = float(v1_row[field])
            v2_value = float(v2_row[field])
            delta = abs(v1_value - v2_value)
            matched = delta <= PARITY_FLOAT_TOLERANCE
            case_match = case_match and matched
            comparisons.append(
                {
                    "field": field,
                    "type": "float",
                    "v1": v1_value,
                    "v2": v2_value,
                    "tolerance": PARITY_FLOAT_TOLERANCE,
                    "matched": matched,
                }
            )
        for field in PARITY_EXACT_FIELDS:
            v1_value = v1_row[field]
            v2_value = v2_row[field]
            matched = v1_value == v2_value
            case_match = case_match and matched
            comparisons.append(
                {
                    "field": field,
                    "type": "exact",
                    "v1": v1_value,
                    "v2": v2_value,
                    "matched": matched,
                }
            )
        failed_fields = [item["field"] for item in comparisons if not bool(item["matched"])]
        comparison_summary = {
            "float_within_tolerance": sum(
                1 for item in comparisons if item["type"] == "float" and bool(item["matched"])
            ),
            "float_failed": sum(
                1 for item in comparisons if item["type"] == "float" and not bool(item["matched"])
            ),
            "exact_matched": sum(
                1 for item in comparisons if item["type"] == "exact" and bool(item["matched"])
            ),
            "exact_failed": sum(
                1 for item in comparisons if item["type"] == "exact" and not bool(item["matched"])
            ),
            "within_tolerance_fields": [
                item["field"] for item in comparisons if item["type"] == "float" and bool(item["matched"])
            ],
            "exact_match_fields": [
                item["field"] for item in comparisons if item["type"] == "exact" and bool(item["matched"])
            ],
            "failed_fields": failed_fields,
        }
        overall_match = overall_match and case_match
        cases_payload.append(
            {
                "name": case.name,
                "reference_on_aligned_rows": case.reference_on_aligned_rows,
                "status": "MATCH" if case_match else "MISMATCH",
                "expected_divergence": [],
                "comparison_summary": comparison_summary,
                "comparisons": comparisons,
            }
        )

    report = {
        "tool": "summary_parity",
        "status": "MATCH" if overall_match else "MISMATCH",
        "evidence_source": "diagnostic",
        "evidence_state": "collected",
        "acceptance_level": "offline_regression",
        "not_real_acceptance_evidence": True,
        "risk_level": "low" if overall_match else "medium",
        "float_tolerance": PARITY_FLOAT_TOLERANCE,
        "float_fields": list(PARITY_FLOAT_FIELDS),
        "exact_fields": list(PARITY_EXACT_FIELDS),
        "tolerance_rules": dict(PARITY_TOLERANCE_RULES),
        "expected_divergence": [],
        "summary": {
            "cases_total": len(cases_payload),
            "cases_matched": sum(1 for case in cases_payload if case["status"] == "MATCH"),
            "cases_failed": sum(1 for case in cases_payload if case["status"] != "MATCH"),
            "failed_cases": [case["name"] for case in cases_payload if case["status"] != "MATCH"],
            "float_within_tolerance": sum(
                int(case["comparison_summary"]["float_within_tolerance"]) for case in cases_payload
            ),
            "float_failed": sum(int(case["comparison_summary"]["float_failed"]) for case in cases_payload),
            "exact_matched": sum(int(case["comparison_summary"]["exact_matched"]) for case in cases_payload),
            "exact_failed": sum(int(case["comparison_summary"]["exact_failed"]) for case in cases_payload),
        },
        "cases": cases_payload,
    }
    json_path = report_dir / "summary_parity_report.json"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    lines = [
        f"# {t('parity.title')}",
        "",
        f"- {t('parity.status')}: {display_compare_status(report['status'], default=str(report['status']))}",
        f"- {t('parity.evidence_source')}: {display_evidence_source(report['evidence_source'], default=str(report['evidence_source']))}",
        f"- {t('parity.acceptance_level')}: {display_acceptance_value(report['acceptance_level'], default=str(report['acceptance_level']))}",
        f"- {t('parity.risk_level')}: {display_risk_level(report['risk_level'], default=str(report['risk_level']))}",
        f"- {t('parity.float_tolerance')}: {PARITY_TOLERANCE_RULES['default_float_abs']}",
        f"- {t('parity.float_fields')}: {', '.join(PARITY_TOLERANCE_RULES['float_fields'])}",
        f"- {t('parity.exact_fields')}: {', '.join(PARITY_TOLERANCE_RULES['exact_fields'])}",
        f"- {t('parity.failed_cases')}: {', '.join(report['summary']['failed_cases']) or t('parity.none')}",
        "",
    ]
    for case in cases_payload:
        lines.append(f"## {case['name']}")
        lines.append(
            f"- {t('parity.status')}: {display_compare_status(case['status'], default=str(case['status']))}"
        )
        lines.append(
            f"- {t('parity.comparison_summary', float_ok=case['comparison_summary']['float_within_tolerance'], float_failed=case['comparison_summary']['float_failed'], exact_ok=case['comparison_summary']['exact_matched'], exact_failed=case['comparison_summary']['exact_failed'])}"
        )
        lines.append(f"- {t('parity.expected_divergence', value=', '.join(case['expected_divergence']) if case['expected_divergence'] else t('parity.none'))}")
        for item in case["comparisons"]:
            suffix = f" tol={item['tolerance']}" if item["type"] == "float" else ""
            lines.append(
                f"- {t('parity.comparison_line', field=item['field'], v1=item['v1'], v2=item['v2'], matched=display_bool(bool(item['matched'])), suffix=suffix)}"
            )
        lines.append("")
    markdown_path = report_dir / "summary_parity_report.md"
    markdown_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    return {
        "status": report["status"],
        "report_dir": str(report_dir),
        "report_json": str(json_path),
        "report_markdown": str(markdown_path),
        "report": report,
    }
