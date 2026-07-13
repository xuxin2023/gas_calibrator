"""Evaluate synthetic V1.5 component-QC fixtures without production side effects."""

from __future__ import annotations

import csv
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .v1_5_component_qc_generator_contract import (
    CONTRACT_SCHEMA,
    validate_v1_5_component_qc_generator_contract,
)


SCHEMA = "v1_5_component_qc_reference_evaluation_v1"
FIXTURE_SCHEMA = "v1_5_component_qc_synthetic_fixture_v1"

GRADE_A = "A_calibration_eligible"
GRADE_B = "B_diagnostic_model_only"
GRADE_C = "C_reject"
GRADE_RANK = {GRADE_A: 0, GRADE_B: 1, GRADE_C: 2}
REVIEW_OUTPUT_SUFFIX = ("docs", "v1_5_flow_contract", "component_qc_reference_evaluator")

FORBIDDEN_FIXTURE_KEYS = {
    "point_dir",
    "historical_root",
    "source_path",
    "source_samples_path",
    "source_frame_qc_path",
    "source_runtime_config_path",
    "com_port",
    "serial_port",
    "device_port",
    "sn_code",
    "device_code",
    "protocol_device_id",
}


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _iter_mapping_keys(value: Any) -> Iterable[str]:
    if isinstance(value, Mapping):
        for key, child in value.items():
            yield str(key)
            yield from _iter_mapping_keys(child)
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for child in value:
            yield from _iter_mapping_keys(child)


def validate_synthetic_component_qc_fixture(fixture: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    if fixture.get("schema") != FIXTURE_SCHEMA:
        reasons.append("fixture_schema_mismatch")
    if fixture.get("synthetic_fixture") is not True:
        reasons.append("synthetic_fixture_flag_required")
    if fixture.get("evidence_source") != "simulated":
        reasons.append("evidence_source_must_be_simulated")
    if fixture.get("not_real_acceptance_evidence") is not True:
        reasons.append("not_real_acceptance_evidence_must_be_true")
    component = str(fixture.get("component") or "").lower()
    if component not in {"co2", "h2o"}:
        reasons.append("component_must_be_co2_or_h2o")
    analyzers = fixture.get("analyzers")
    if not isinstance(analyzers, list) or not analyzers:
        reasons.append("active_analyzers_required")
    else:
        labels: set[str] = set()
        prefixes: set[str] = set()
        for analyzer in analyzers:
            if not isinstance(analyzer, Mapping):
                reasons.append("analyzer_row_must_be_object")
                continue
            label = str(analyzer.get("label") or "").strip()
            prefix = str(analyzer.get("prefix") or "").strip()
            if not label:
                reasons.append("analyzer_label_required")
            if not prefix:
                reasons.append("analyzer_prefix_required")
            if label in labels:
                reasons.append(f"duplicate_analyzer_label:{label}")
            if prefix in prefixes:
                reasons.append(f"duplicate_analyzer_prefix:{prefix}")
            labels.add(label)
            prefixes.add(prefix)
    sample_rows = fixture.get("sample_rows")
    if not isinstance(sample_rows, list):
        reasons.append("sample_rows_must_be_list")
    elif any(not isinstance(row, Mapping) for row in sample_rows):
        reasons.append("sample_row_must_be_object")
    forbidden = sorted(FORBIDDEN_FIXTURE_KEYS.intersection(_iter_mapping_keys(fixture)))
    reasons.extend(f"historical_or_device_field_forbidden:{key}" for key in forbidden)
    return sorted(set(reasons))


def _finite_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _worse_grade(left: str, right: str) -> str:
    return left if GRADE_RANK[left] >= GRADE_RANK[right] else right


def _at_or_below(value: float, threshold: float) -> bool:
    return value <= threshold or math.isclose(value, threshold, rel_tol=0.0, abs_tol=1e-12)


def _grade_semantics(contract: Mapping[str, Any], grade: str) -> tuple[bool, bool]:
    row = (contract.get("common_grade_contract") or {}).get(grade) or {}
    return (
        row.get("sample_can_enter_calibration_fit") is True,
        row.get("sample_can_enter_diagnostic_model") is True,
    )


def _point_blockers(fixture: Mapping[str, Any], contract: Mapping[str, Any]) -> list[str]:
    flags = fixture.get("point_flags") or {}
    return sorted(
        blocker
        for blocker in contract.get("point_wide_hard_blockers") or []
        if flags.get(blocker) is True
    )


def _analyzer_temporal_evidence(fixture: Mapping[str, Any], prefix: str) -> Mapping[str, Any]:
    by_analyzer = fixture.get("analyzer_evidence") or {}
    specific = by_analyzer.get(prefix) or {}
    return {
        "temporal_window_complete": specific.get(
            "temporal_window_complete", fixture.get("temporal_window_complete", True)
        ),
        "cadence_warning": specific.get("cadence_warning", fixture.get("cadence_warning", False)),
    }


def _evaluate_analyzer(
    *,
    fixture: Mapping[str, Any],
    contract: Mapping[str, Any],
    analyzer: Mapping[str, Any],
    component: str,
    required_count: int,
    hard_blockers: list[str],
    source_hashes: Mapping[str, str],
) -> dict[str, Any]:
    label = str(analyzer["label"])
    prefix = str(analyzer["prefix"])
    route = (contract.get("routes") or {})[component]
    ratio_key = f"{prefix}_{route['ratio_key_suffix']}"
    usable_key = f"{prefix}_frame_usable"
    rows = fixture.get("sample_rows") or []
    reasons: list[str] = []
    grade = GRADE_A

    temporal = _analyzer_temporal_evidence(fixture, prefix)
    timestamps_complete = all(_finite_float(row.get("timestamp_s")) is not None for row in rows)
    if temporal.get("temporal_window_complete") is not True or not timestamps_complete:
        grade = GRADE_C
        reasons.append("missing_timestamps_or_incomplete_window")

    ratios: list[float] = []
    invalid_usable_ratio = False
    for row in rows:
        if row.get(usable_key) is not True:
            continue
        ratio = _finite_float(row.get(ratio_key))
        if ratio is None:
            invalid_usable_ratio = True
        else:
            ratios.append(ratio)
    if invalid_usable_ratio:
        grade = GRADE_C
        reasons.append("missing_or_nonfinite_ratio_in_usable_frame")

    b_minimum = math.ceil(
        required_count * float((contract.get("frame_count_contract") or {})["B_minimum_usable_count_fraction"])
    )
    if len(ratios) < b_minimum:
        grade = GRADE_C
        reasons.append(f"usable_ratio_count_below_minimum:{len(ratios)}<{b_minimum}")
    elif len(ratios) < required_count:
        grade = _worse_grade(grade, GRADE_B)
        reasons.append(f"usable_ratio_count_below_required:{len(ratios)}<{required_count}")

    ratio_span: float | None = None
    if ratios:
        ratio_span = round(max(ratios) - min(ratios), 12)
        a_max = float(route["A_ratio_span_max"])
        if not _at_or_below(ratio_span, a_max):
            if component == "co2":
                b_max = float(route["B_ratio_span_max"])
                if _at_or_below(ratio_span, b_max):
                    grade = _worse_grade(grade, GRADE_B)
                    reasons.append("co2_ratio_span_above_a_within_b")
                else:
                    grade = GRADE_C
                    reasons.append("co2_ratio_span_above_b")
            else:
                grade = _worse_grade(grade, GRADE_B)
                reasons.append("h2o_ratio_span_above_a_diagnostic_only")

    if temporal.get("cadence_warning") is True and grade != GRADE_C:
        grade = _worse_grade(grade, GRADE_B)
        reasons.append("cadence_warning_grade_capped_at_b")

    if hard_blockers:
        grade = GRADE_C
        reasons.extend(f"point_wide_hard_blocker:{item}" for item in hard_blockers)

    fit_allowed, diagnostic_allowed = _grade_semantics(contract, grade)
    return {
        "label": label,
        "prefix": prefix,
        "grade": grade,
        "ratio_key": ratio_key,
        "ratio_span": ratio_span,
        "ratio_a_tol": route.get("A_ratio_span_max"),
        "ratio_hard_tol": route.get("B_ratio_span_max"),
        "frame_count": len(rows),
        "usable_ratio_count": len(ratios),
        "required_sample_count": required_count,
        "reason": ";".join(sorted(set(reasons))) if reasons else "within_reference_contract",
        "sample_can_enter_calibration_fit": fit_allowed,
        "sample_can_enter_diagnostic_model": diagnostic_allowed,
        **source_hashes,
    }


def evaluate_v1_5_component_qc_reference_fixture(
    fixture: Mapping[str, Any], contract: Mapping[str, Any]
) -> dict[str, Any]:
    """Return a deterministic component-QC model for one synthetic fixture."""

    fixture_reasons = validate_synthetic_component_qc_fixture(fixture)
    contract_reasons = validate_v1_5_component_qc_generator_contract(contract)
    if contract.get("schema") != CONTRACT_SCHEMA:
        contract_reasons.append("contract_schema_mismatch")
    reasons = sorted(set(fixture_reasons + contract_reasons))
    if reasons:
        raise ValueError(";".join(reasons))

    component = str(fixture["component"]).lower()
    required_count = fixture.get("required_sample_count")
    if required_count is None:
        required_count = (contract.get("scope") or {}).get("default_required_sample_count")
    if (
        isinstance(required_count, bool)
        or not isinstance(required_count, int)
        or required_count <= 0
    ):
        raise ValueError("required_sample_count_must_be_positive_integer")

    sample_rows = fixture.get("sample_rows") or []
    frame_subset = [
        {
            key: value
            for key, value in row.items()
            if key == "timestamp_s" or key.endswith("_frame_usable")
        }
        for row in sample_rows
    ]
    runtime_subset = {
        "component": component,
        "required_sample_count": required_count,
        "analyzers": fixture.get("analyzers"),
        "point_flags": fixture.get("point_flags") or {},
        "temporal_window_complete": fixture.get("temporal_window_complete", True),
        "cadence_warning": fixture.get("cadence_warning", False),
        "analyzer_evidence": fixture.get("analyzer_evidence") or {},
    }
    source_hashes = {
        "source_samples_sha256": _sha256(sample_rows),
        "source_frame_qc_sha256": _sha256(frame_subset),
        "source_runtime_config_sha256": _sha256(runtime_subset),
        "contract_sha256": _sha256(contract),
    }
    hard_blockers = _point_blockers(fixture, contract)
    analyzer_rows = [
        _evaluate_analyzer(
            fixture=fixture,
            contract=contract,
            analyzer=analyzer,
            component=component,
            required_count=required_count,
            hard_blockers=hard_blockers,
            source_hashes=source_hashes,
        )
        for analyzer in fixture["analyzers"]
    ]
    worst_grade = max((row["grade"] for row in analyzer_rows), key=GRADE_RANK.__getitem__)
    return {
        "schema": SCHEMA,
        "overall_status": "synthetic_reference_evaluation_complete",
        "production_state": "reference_evaluator_only_production_generator_blocked",
        "component": component,
        "point_id": fixture.get("point_id"),
        "synthetic_fixture": True,
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "required_sample_count": required_count,
        "sample_alignment_ok": fixture.get("sample_alignment_ok"),
        "point_wide_hard_blockers": hard_blockers,
        "analyzers": analyzer_rows,
        "point_summary": {
            "informational_only": True,
            "worst_grade": worst_grade,
            "calibration_eligible_analyzer_count": sum(
                row["sample_can_enter_calibration_fit"] is True for row in analyzer_rows
            ),
            "diagnostic_analyzer_count": sum(
                row["sample_can_enter_diagnostic_model"] is True for row in analyzer_rows
            ),
            "one_analyzer_failure_blocks_other_analyzers": False,
        },
        "fixture_sha256": _sha256(fixture),
        **source_hashes,
        "locks": {
            "reference_evaluator_available": True,
            "production_component_qc_generator_available": False,
            "historical_component_qc_generation_allowed": False,
            "historical_component_qc_write_allowed": False,
            "component_qc_backfill_allowed": False,
            "historical_fit_allowed": False,
            "formal_release_allowed": False,
            "database_import_allowed": False,
            "opens_com_ports": False,
            "controls_pressure": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
            "writes_sn_or_device_code": False,
            "connects_postgresql": False,
        },
    }


def write_v1_5_component_qc_reference_evaluation(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    """Write review-only artifacts to an explicit output directory."""

    out = Path(output_dir).resolve()
    suffix = tuple(part.lower() for part in out.parts[-len(REVIEW_OUTPUT_SUFFIX) :])
    if suffix != REVIEW_OUTPUT_SUFFIX:
        raise ValueError("output_dir_must_be_component_qc_reference_review_directory")
    out.mkdir(parents=True, exist_ok=True)
    outputs = {
        "json": out / "v1_5_component_qc_reference_evaluation.json",
        "analyzer_csv": out / "v1_5_component_qc_reference_evaluation_by_analyzer.csv",
        "markdown": out / "V1_5_COMPONENT_QC_REFERENCE_EVALUATION.md",
    }
    outputs["json"].write_text(
        json.dumps(dict(model), ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    analyzer_rows = list(model.get("analyzers") or [])
    fieldnames = list(analyzer_rows[0]) if analyzer_rows else ["label", "grade"]
    with outputs["analyzer_csv"].open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(analyzer_rows)
    summary = model.get("point_summary") or {}
    lines = [
        "# V1.5 Component-QC Synthetic Reference Evaluation",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- component: `{model.get('component')}`",
        f"- point_id: `{model.get('point_id')}`",
        f"- worst_grade (informational only): `{summary.get('worst_grade')}`",
        f"- calibration_eligible_analyzer_count: `{summary.get('calibration_eligible_analyzer_count')}`",
        "- evidence_source: `simulated`",
        "- not_real_acceptance_evidence: `true`",
        "- historical_component_qc_write_allowed: `false`",
        "- opens_com_ports: `false`",
        "",
        "## Per-analyzer result",
        "",
        "| label | grade | ratio_span | fit | reason |",
        "|---|---|---:|---|---|",
    ]
    lines.extend(
        f"| {row['label']} | {row['grade']} | {row['ratio_span']} | "
        f"{str(row['sample_can_enter_calibration_fit']).lower()} | {row['reason']} |"
        for row in analyzer_rows
    )
    outputs["markdown"].write_text("\n".join(lines) + "\n", encoding="utf-8")
    return outputs


__all__ = [
    "FIXTURE_SCHEMA",
    "REVIEW_OUTPUT_SUFFIX",
    "SCHEMA",
    "evaluate_v1_5_component_qc_reference_fixture",
    "validate_synthetic_component_qc_fixture",
    "write_v1_5_component_qc_reference_evaluation",
]
