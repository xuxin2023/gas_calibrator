"""Sidecar artifacts for formal V1.5 open-flow calibration evidence.

The helpers in this module only read historical run artifacts and write a
validation report. They do not control valves, pressure controllers, analyzers,
or coefficient storage.
"""

from __future__ import annotations

import json
import re
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from .artifact_rows import load_latest_sample_rows, normalize_sample_row
from .common import latest_artifact, load_csv_rows
from .formal_open_flow import (
    FormalOpenFlowConfig,
    build_formal_open_flow_report,
    report_to_dict,
)
from .pressure_channel import validate_pressure_reference_traceability
from .reporting import ValidationMetadata, write_validation_report


def _compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _table_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (dict, list, tuple)):
        return _compact_json(value)
    return value


def detect_analyzer_prefixes(rows: Sequence[Mapping[str, Any]]) -> List[str]:
    """Detect analyzer acquisition-channel prefixes present in sample rows."""

    prefixes: set[str] = set()
    for row in rows:
        for identity_key in ("analyzer_prefix", "acquisition_channel"):
            prefix_value = str(row.get(identity_key) or "").strip().lower()
            if re.match(r"^ga\d{2,}$", prefix_value):
                prefixes.add(prefix_value)
        for key, value in row.items():
            text = str(key or "").strip().lower()
            match = re.match(r"^(ga\d{2,})_", text)
            if not match:
                continue
            if value in (None, ""):
                continue
            prefixes.add(match.group(1))
    return sorted(prefixes) or ["ga01"]


def resolve_analyzer_prefixes(
    rows: Sequence[Mapping[str, Any]],
    analyzer_prefix: str = "ga01",
) -> List[str]:
    """Resolve a single prefix, comma list, or auto/all request."""

    text = str(analyzer_prefix or "ga01").strip()
    lowered = text.lower()
    if lowered in {"*", "all", "auto", "detected"}:
        return detect_analyzer_prefixes(rows)
    parts = [part.strip().lower() for part in text.split(",") if part.strip()]
    return parts or ["ga01"]


def load_pressure_check_rows(
    run_dir: str | Path,
    *,
    fallback_rows: Sequence[Mapping[str, Any]],
    pressure_check_path: str | Path | None = None,
) -> tuple[str, List[Dict[str, Any]], Optional[Path]]:
    root = Path(run_dir)
    if pressure_check_path is not None:
        explicit = Path(pressure_check_path)
        if explicit.is_dir():
            for pattern in (
                "pressure_channel_quick_check*.csv",
                "*pressure_channel_quick_check*.csv",
                "pressure_quick_check*.csv",
                "*pressure_quick_check*.csv",
            ):
                path = latest_artifact(explicit, pattern)
                if path is not None:
                    rows = [normalize_sample_row(row) for row in load_csv_rows(path)]
                    return "external_pressure_quick_check_artifact", rows, path
            raise FileNotFoundError(f"Pressure quick-check CSV not found in: {explicit}")
        if not explicit.exists():
            raise FileNotFoundError(f"Pressure quick-check CSV not found: {explicit}")
        rows = [normalize_sample_row(row) for row in load_csv_rows(explicit)]
        return "external_pressure_quick_check_artifact", rows, explicit
    for pattern in (
        "pressure_channel_quick_check*.csv",
        "*pressure_channel_quick_check*.csv",
        "pressure_quick_check*.csv",
        "*pressure_quick_check*.csv",
    ):
        path = latest_artifact(root, pattern)
        if path is not None:
            rows = [normalize_sample_row(row) for row in load_csv_rows(path)]
            return "pressure_quick_check_artifact", rows, path
    return "sample_rows_fallback", [dict(row) for row in fallback_rows], None


def load_plan_snapshot(path: str | Path | None) -> Dict[str, Any]:
    if path is None:
        return {}
    plan_path = Path(path)
    if not plan_path.exists():
        raise FileNotFoundError(f"Plan snapshot not found: {plan_path}")
    return json.loads(plan_path.read_text(encoding="utf-8"))


def load_pressure_reference_snapshot(path: str | Path | None) -> Dict[str, Any]:
    if path is None:
        return {}
    reference_path = Path(path)
    if not reference_path.exists():
        raise FileNotFoundError(f"Pressure reference snapshot not found: {reference_path}")
    return json.loads(reference_path.read_text(encoding="utf-8"))


def _component_from_row(row: Mapping[str, Any]) -> str:
    values = [
        row.get("point_phase"),
        row.get("route"),
        row.get("gas_type"),
        row.get("step"),
        row.get("point_tag"),
        row.get("point_title"),
    ]
    text = " ".join(str(value or "").lower() for value in values)
    if any(marker in text for marker in ("h2o", "水路", "湿气", "水浓度")):
        return "h2o"
    if any(marker in text for marker in ("co2", "气路", "干气", "二氧化碳")):
        return "co2"
    return ""


def _has_component_payload(row: Mapping[str, Any], component: str, analyzer_prefix: str) -> bool:
    if component == "h2o":
        keys = (f"{analyzer_prefix}_h2o_ratio_f", "h2o_ratio_f", f"{analyzer_prefix}_h2o_mmol", "h2o_mmol")
    else:
        keys = (f"{analyzer_prefix}_co2_ratio_f", "co2_ratio_f", f"{analyzer_prefix}_co2_ppm", "co2_ppm")
    return any(row.get(key) not in (None, "") for key in keys)


def _row_matches_analyzer_prefix(row: Mapping[str, Any], analyzer_prefix: str) -> bool:
    requested = str(analyzer_prefix or "").strip().lower()
    if any(str(key or "").strip().lower().startswith(f"{requested}_") for key in row):
        return True
    row_prefix = str(row.get("analyzer_prefix") or row.get("acquisition_channel") or "").strip().lower()
    if row_prefix:
        return row_prefix == requested
    return True


def select_component_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    component: str,
    analyzer_prefix: str = "ga01",
) -> List[Dict[str, Any]]:
    selected = [
        dict(row)
        for row in rows
        if _component_from_row(row) == component
        and _row_matches_analyzer_prefix(row, analyzer_prefix)
    ]
    if selected:
        return selected
    return [
        dict(row)
        for row in rows
        if _has_component_payload(row, component, analyzer_prefix)
        and _component_from_row(row) in {"", component}
        and _row_matches_analyzer_prefix(row, analyzer_prefix)
    ]


def _distinct_point_identity_count(rows: Sequence[Mapping[str, Any]], component: str) -> int:
    identities: set[str] = set()
    component_key = str(component or "").strip().lower()
    target_keys = (
        ("target_h2o_mmol", "certificate_h2o_mmol", "h2o_mmol")
        if component_key == "h2o"
        else ("target_co2_ppm", "certificate_co2_ppm", "co2_ppm")
    )
    for row in rows:
        identity = (
            row.get("point_id")
            or row.get("point_tag")
            or row.get("point_title")
            or next((row.get(key) for key in target_keys if row.get(key) not in (None, "")), None)
        )
        if identity not in (None, ""):
            identities.add(str(identity))
    return len(identities)


def _summary_row(
    component: str,
    report: Mapping[str, Any],
    pressure_source: str,
    pressure_traceability: Mapping[str, Any],
) -> Dict[str, Any]:
    pressure = report.get("pressure_channel_quick_check") or {}
    qc = report.get("qc_summary") or {}
    readiness = report.get("sample_readiness") or {}
    calibratability = report.get("point_calibratability") or {}
    return {
        "component": component,
        "analyzer_prefix": pressure.get("analyzer_prefix") or qc.get("analyzer_prefix") or "",
        "analyzer_device_id": pressure.get("analyzer_device_id") or qc.get("analyzer_device_id") or "",
        "analyzer_identity_source": pressure.get("analyzer_identity_source") or "",
        "state_sequence": ";".join(report.get("state_sequence") or []),
        "plan_status": report.get("plan_status", ""),
        "plan_reasons": ";".join(report.get("plan_reasons") or []),
        "pressure_channel_quick_check_status": pressure.get("status", ""),
        "pressure_channel_quick_check_reason": pressure.get("reason", ""),
        "pressure_check_source": pressure_source,
        "pressure_reference_traceability_status": pressure_traceability.get("status", ""),
        "pressure_reference_validation_level": pressure_traceability.get("validation_level", ""),
        "pressure_reference_traceability_reasons": ";".join(
            pressure_traceability.get("reasons") or []
        ),
        "total_samples": qc.get("total_samples", 0),
        "required_sample_count": qc.get("required_sample_count", 0),
        "sampling_completion_status": qc.get("sampling_completion_status", ""),
        "sampling_completion_reason": qc.get("sampling_completion_reason", ""),
        "mode2_present_count": qc.get("mode2_present_count", 0),
        "component_payload_count": qc.get("component_payload_count", 0),
        "a_grade_count": qc.get("a_grade_count", 0),
        "b_grade_count": qc.get("b_grade_count", 0),
        "rejected_count": qc.get("rejected_count", 0),
        "sample_readiness_status": readiness.get("readiness_status", ""),
        "sample_readiness_blockers": ";".join(readiness.get("blockers") or []),
        "sample_readiness_warnings": ";".join(readiness.get("warnings") or []),
        "point_calibratability_grade": calibratability.get("calibratability_grade", ""),
        "point_calibratability_role": calibratability.get("fit_input_role", ""),
        "time_optimization_action": calibratability.get("time_optimization_action", ""),
        "point_calibratability_reasons": ";".join(calibratability.get("reasons") or []),
        "point_calibratability_warnings": ";".join(
            calibratability.get("warnings") or []
        ),
        "pressure_condition_warning_count": qc.get("pressure_condition_warning_count", 0),
        "window_report_warnings": _table_value(qc.get("window_report_warnings") or {}),
        "candidate_fit_allowed": report.get("candidate_fit_allowed", False),
        "candidate_fit_blockers": ";".join(report.get("candidate_fit_blockers") or []),
        "formal_fit_boundary": _table_value(report.get("formal_fit_boundary") or {}),
    }


def _prefix_component(rows: Iterable[Mapping[str, Any]], component: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        item = {"component": component}
        item.update({str(key): _table_value(value) for key, value in row.items()})
        out.append(item)
    return out


def build_formal_open_flow_tables(
    *,
    run_dir: str | Path,
    plan: Mapping[str, Any],
    component: str = "both",
    analyzer_prefix: str = "ga01",
    cfg: Optional[FormalOpenFlowConfig] = None,
    pressure_reference: Optional[Mapping[str, Any]] = None,
    pressure_check_path: str | Path | None = None,
    today: Any = None,
) -> tuple[Dict[str, List[Dict[str, Any]]], Dict[str, Any]]:
    samples_path, sample_rows = load_latest_sample_rows(run_dir)
    pressure_source, pressure_rows, pressure_path = load_pressure_check_rows(
        run_dir,
        fallback_rows=sample_rows,
        pressure_check_path=pressure_check_path,
    )
    pressure_traceability = validate_pressure_reference_traceability(
        pressure_reference or {},
        today=today,
    )
    pressure_traceability_dict = asdict(pressure_traceability)
    components = ("co2", "h2o") if component == "both" else (component,)
    analyzer_prefixes = resolve_analyzer_prefixes(sample_rows, analyzer_prefix)

    tables: Dict[str, List[Dict[str, Any]]] = {
        "run_summary": [],
        "sampling_completion": [],
        "pressure_reference_traceability": [
            {str(key): _table_value(value) for key, value in pressure_traceability_dict.items()}
        ],
        "pressure_check": [],
        "sample_readiness": [],
        "point_calibratability": [],
        "a_grade_samples": [],
        "b_grade_review_samples": [],
        "rejected_samples": [],
        "formal_fit_boundary": [],
    }
    component_counts: Dict[str, int] = {}

    for prefix in analyzer_prefixes:
        for item in components:
            rows = select_component_rows(sample_rows, component=item, analyzer_prefix=prefix)
            component_counts[f"{prefix}:{item}"] = len(rows)
            report = build_formal_open_flow_report(
                plan=plan,
                sample_rows=rows,
                component=item,
                analyzer_prefix=prefix,
                cfg=cfg,
                pressure_check_rows=pressure_rows,
            )
            report_dict = report_to_dict(report)
            point_identity_count = _distinct_point_identity_count(rows, item)
            blockers = list(report_dict.get("candidate_fit_blockers") or [])
            if point_identity_count > 1 and "point_not_calibratable" in blockers:
                # Multi-point candidate packages must not reuse the single-point
                # stability span across different gas/humidity targets. Per-point
                # readiness is recorded by each sidecar run; this package-level
                # report only decides whether A-grade rows can enter coefficient
                # review.
                blockers = [item for item in blockers if item != "point_not_calibratable"]
                report_dict["candidate_fit_blockers"] = blockers
                if not blockers:
                    report_dict["candidate_fit_allowed"] = True
            if pressure_traceability.status != "pass":
                report_dict["candidate_fit_allowed"] = False
                blockers = list(report_dict.get("candidate_fit_blockers") or [])
                if "pressure_reference_traceability_failed" not in blockers:
                    blockers.append("pressure_reference_traceability_failed")
                report_dict["candidate_fit_blockers"] = blockers
            tables["run_summary"].append(
                _summary_row(item, report_dict, pressure_source, pressure_traceability_dict)
            )
            qc = report_dict.get("qc_summary") or {}
            tables["sampling_completion"].append(
                {
                    "component": item,
                    "analyzer_prefix": prefix,
                    "analyzer_device_id": qc.get("analyzer_device_id", ""),
                    "completion_status": qc.get("sampling_completion_status", ""),
                    "completion_reason": qc.get("sampling_completion_reason", ""),
                    "total_samples": qc.get("total_samples", 0),
                    "mode2_present_count": qc.get("mode2_present_count", 0),
                    "component_payload_count": qc.get("component_payload_count", 0),
                    "a_grade_count": qc.get("a_grade_count", 0),
                    "rejected_count": qc.get("rejected_count", 0),
                }
            )
            pressure_row = {"component": item}
            pressure_row.update(
                {
                    str(key): _table_value(value)
                    for key, value in (report_dict.get("pressure_channel_quick_check") or {}).items()
                }
            )
            tables["pressure_check"].append(pressure_row)
            readiness_row = {"component": item, "analyzer_prefix": prefix}
            readiness_row.update(
                {
                    str(key): _table_value(value)
                    for key, value in (report_dict.get("sample_readiness") or {}).items()
                }
            )
            tables["sample_readiness"].append(readiness_row)
            calibratability_row = {"component": item, "analyzer_prefix": prefix}
            calibratability_row.update(
                {
                    str(key): _table_value(value)
                    for key, value in (
                        report_dict.get("point_calibratability") or {}
                    ).items()
                }
            )
            tables["point_calibratability"].append(calibratability_row)
            tables["a_grade_samples"].extend(_prefix_component(report_dict.get("a_grade_samples") or [], item))
            tables["b_grade_review_samples"].extend(
                _prefix_component(report_dict.get("b_grade_samples") or [], item)
            )
            tables["rejected_samples"].extend(_prefix_component(report_dict.get("rejected_samples") or [], item))
            boundary = {"component": item}
            boundary.update(
                {
                    str(key): _table_value(value)
                    for key, value in (report_dict.get("formal_fit_boundary") or {}).items()
                }
            )
            tables["formal_fit_boundary"].append(boundary)

    context = {
        "samples_path": str(samples_path),
        "pressure_check_source": pressure_source,
        "pressure_check_path": str(pressure_path) if pressure_path else "",
        "component_counts": component_counts,
        "analyzer_prefix": analyzer_prefix,
        "analyzer_prefixes": analyzer_prefixes,
        "pressure_reference_traceability": pressure_traceability_dict,
    }
    return tables, context


def write_formal_open_flow_sidecar_report(
    *,
    run_dir: str | Path,
    output_dir: str | Path | None = None,
    plan: Optional[Mapping[str, Any]] = None,
    plan_path: str | Path | None = None,
    pressure_reference: Optional[Mapping[str, Any]] = None,
    pressure_reference_path: str | Path | None = None,
    pressure_check_path: str | Path | None = None,
    component: str = "both",
    analyzer_prefix: str = "ga01",
    cfg: Optional[FormalOpenFlowConfig] = None,
    today: Any = None,
) -> Dict[str, Path]:
    root = Path(run_dir).resolve()
    plan_data = dict(plan) if plan is not None else load_plan_snapshot(plan_path)
    pressure_reference_data = (
        dict(pressure_reference)
        if pressure_reference is not None
        else load_pressure_reference_snapshot(pressure_reference_path)
    )
    tables, context = build_formal_open_flow_tables(
        run_dir=root,
        plan=plan_data,
        component=component,
        analyzer_prefix=analyzer_prefix,
        cfg=cfg,
        pressure_reference=pressure_reference_data,
        pressure_check_path=pressure_check_path,
        today=today,
    )
    destination = Path(output_dir).resolve() if output_dir else root / "formal_open_flow_report"
    metadata = ValidationMetadata(
        tool_name="export_v1_5_formal_open_flow_report",
        created_at=datetime.now().isoformat(timespec="seconds"),
        analyzers=list(context.get("analyzer_prefixes") or [analyzer_prefix]),
        input_paths=[
            context["samples_path"],
            context.get("pressure_check_path", ""),
            str(Path(plan_path).resolve()) if plan_path else "",
            str(Path(pressure_reference_path).resolve()) if pressure_reference_path else "",
        ],
        output_dir=str(destination),
        config_summary={
            "component": component,
            "analyzer_prefix": analyzer_prefix,
            "analyzer_prefixes": context.get("analyzer_prefixes", []),
            "pressure_check_source": context["pressure_check_source"],
            "component_counts": context["component_counts"],
            "pressure_reference_traceability": context["pressure_reference_traceability"],
        },
        notes=[
            "Sidecar-only formal open-flow evidence export.",
            "No COM ports are opened and no valve, PACE, analyzer, or coefficient writes are performed.",
            "A-grade rows are the only default candidate-fit inputs; B-grade rows require reviewer decision.",
            "Sealed pressure points, dynamic control probes, PACE continuous sink, and VENT-hold remain diagnostic-only by default.",
        ],
    )
    prefix = "formal_open_flow"
    return write_validation_report(destination, prefix=prefix, metadata=metadata, tables=tables)
