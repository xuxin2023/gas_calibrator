from __future__ import annotations

import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..sim.replay import DEFAULT_REPLAY_FIXTURE_ROOT, list_replay_scenarios, load_replay_fixture
from ..sim.scenarios.catalog import SCENARIOS, SimulatedScenarioDefinition


GOLDEN_DATASET_REGISTRY_SCHEMA_VERSION = "golden-dataset-registry-v1"
GOLDEN_DATASET_REGISTRY_FILENAME = "golden_dataset_registry.json"

_TEMP_PATTERN = re.compile(r"(?P<value>-?\d+(?:\.\d+)?)c(?=_|$)", re.IGNORECASE)
_PRESSURE_PATTERN = re.compile(r"(?P<value>\d+(?:\.\d+)?)hpa(?=_|$)", re.IGNORECASE)

_BOUNDARY_FIELDS: dict[str, Any] = {
    "evidence_source": "simulated",
    "reviewer_only": True,
    "readiness_mapping_only": True,
    "not_real_acceptance_evidence": True,
    "not_formal_metrology_conclusion": True,
}

_ANOMALY_KEYWORDS: dict[str, tuple[str, ...]] = {
    "nominal_success": ("success", "stable_reference"),
    "partial_frame": ("partial_frame", "mode2"),
    "relay_stuck": ("stuck_channel", "relay_stuck"),
    "humidity_timeout": ("humidity_generator_timeout", "wait_humidity", "humidity never converges"),
    "pace_cleanup_no_response": ("cleanup_no_response", "pace_no_response"),
    "pace_unsupported_header": ("unsupported_header", "undefined-header"),
    "pressure_reference_timeout": ("gauge_no_response", "reference read timeout", "pressure_reference_degraded"),
    "pressure_unit_misconfigured": ("wrong_unit_configuration", "wrong unit"),
    "temperature_chamber_stalled": ("temperature_chamber_stalled", "chamber_stalled"),
    "thermometer_stale": ("thermometer_stale", "stale_reference"),
    "thermometer_no_response": ("thermometer_no_response", "stops streaming"),
    "serial_port_locked": ("serial_port_busy", "resource_locked_serial_port", "port lock"),
    "latest_index_guardrail": ("latest_missing", "not_primary", "snapshot_only"),
    "sample_count_mismatch": ("sample_count_mismatch",),
    "route_trace_partial_artifacts": ("partial_artifacts",),
}


def build_golden_dataset_registry(*, replay_root: Path | None = None) -> dict[str, Any]:
    fixture_root = Path(replay_root or DEFAULT_REPLAY_FIXTURE_ROOT)
    cases: dict[str, dict[str, Any]] = {}

    for definition in SCENARIOS.values():
        cases[definition.name] = _build_case_from_scenario(definition)

    for scenario_name in list_replay_scenarios(root=fixture_root):
        payload = load_replay_fixture(scenario=scenario_name, root=fixture_root)
        existing = cases.get(scenario_name)
        fixture_case = _build_case_from_replay_fixture(scenario_name, payload)
        if existing is None:
            cases[scenario_name] = fixture_case
            continue
        cases[scenario_name] = _merge_case_with_fixture(existing, fixture_case)

    ordered_cases = [cases[name] for name in sorted(cases)]
    summary = {
        "total_cases": len(ordered_cases),
        "source_kind_counts": _count_scalar_rows(ordered_cases, "source_kinds"),
        "gas_family_counts": _count_scalar_rows(ordered_cases, "gas_families"),
        "path_category_counts": _count_scalar_rows(ordered_cases, "path_categories"),
        "temperature_point_category_counts": _count_scalar_rows(ordered_cases, "temperature_point_categories"),
        "pressure_point_category_counts": _count_scalar_rows(ordered_cases, "pressure_point_categories"),
        "analyzer_population_counts": Counter(
            str(case.get("analyzer_population_category") or "unknown") for case in ordered_cases
        ),
        "chain_length_counts": Counter(
            str(case.get("chain_length_category") or "unspecified") for case in ordered_cases
        ),
        "anomaly_counts": _count_scalar_rows(ordered_cases, "anomaly_scenarios"),
        "diagnostic_only_cases": sum(1 for case in ordered_cases if bool(case.get("diagnostic_only"))),
        "acceptance_like_cases": sum(1 for case in ordered_cases if bool(case.get("acceptance_like_coverage"))),
    }
    return {
        "schema_version": GOLDEN_DATASET_REGISTRY_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "fixture_root": str(fixture_root),
        "summary": _normalize_counter_payload(summary),
        "cases": ordered_cases,
        **_BOUNDARY_FIELDS,
    }


def write_golden_dataset_registry(output_dir: str | Path, registry: dict[str, Any]) -> Path:
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / GOLDEN_DATASET_REGISTRY_FILENAME
    path.write_text(json.dumps(registry, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def _build_case_from_scenario(definition: SimulatedScenarioDefinition) -> dict[str, Any]:
    device_matrix = definition.device_matrix.to_dict()
    base = {
        "case_id": definition.name,
        "display_name": definition.name,
        "source_kinds": ["scenario_definition"],
        "source_refs": {
            "scenario_definition": f"sim.scenarios.catalog::{definition.name}",
        },
        "validation_profile": definition.validation_profile,
        "target_route": definition.target_route,
        "description": definition.description,
        "diagnostic_only": bool(definition.diagnostic_only),
        "acceptance_like_coverage": not bool(definition.diagnostic_only),
        "execution_mode": definition.execution_mode,
        "baseline_mode": definition.baseline_mode,
        "gas_families": _gas_families(definition.target_route),
        "path_categories": _path_categories(
            name=definition.name,
            description=definition.description,
            target_route=definition.target_route,
            route_mode="",
        ),
        "temperature_points_c": [],
        "temperature_point_categories": [],
        "pressure_points_hpa": [],
        "pressure_point_categories": [],
        "analyzer_population_category": _analyzer_population_category(device_matrix),
        "chain_length_category": _chain_length_category(device_matrix, definition.target_route),
        "anomaly_scenarios": _anomaly_scenarios(
            name=definition.name,
            description=definition.description,
            device_matrix=device_matrix,
            payload={},
        ),
        "device_matrix": device_matrix,
        **_BOUNDARY_FIELDS,
    }
    return base


def _build_case_from_replay_fixture(case_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    trace_metrics = _trace_metrics(payload)
    route_mode = str(payload.get("route_mode") or "")
    target_route = str(
        dict(payload.get("bench_context") or {}).get("target_route")
        or route_mode
        or trace_metrics.get("target_route")
        or ""
    )
    description = str(payload.get("description") or payload.get("summary") or "")
    path_categories = _path_categories(
        name=case_id,
        description=description,
        target_route=target_route,
        route_mode=route_mode,
    )
    if trace_metrics["environment_markers"]:
        path_categories = sorted(set(path_categories) | {"environment"})
    return {
        "case_id": case_id,
        "display_name": case_id,
        "source_kinds": ["replay_fixture"],
        "source_refs": {
            "replay_fixture": str(payload.get("_fixture_path") or ""),
        },
        "validation_profile": str(payload.get("validation_profile") or ""),
        "target_route": target_route,
        "description": description,
        "diagnostic_only": bool(dict(payload.get("bench_context") or {}).get("diagnostic_only", False)),
        "acceptance_like_coverage": bool(
            dict(payload.get("bench_context") or {}).get("validation_role")
            == "simulated_acceptance_like_coverage"
        ),
        "execution_mode": "replay_fixture",
        "baseline_mode": "fixture_snapshot",
        "gas_families": sorted(set(trace_metrics["gas_families"]) | set(_gas_families(target_route))),
        "path_categories": path_categories,
        "temperature_points_c": trace_metrics["temperatures_c"],
        "temperature_point_categories": _temperature_categories(trace_metrics["temperatures_c"]),
        "pressure_points_hpa": trace_metrics["pressures_hpa"],
        "pressure_point_categories": _pressure_categories(
            trace_metrics["pressures_hpa"],
            has_ambient=trace_metrics["has_ambient"],
        ),
        "analyzer_population_category": "single_or_unspecified",
        "chain_length_category": "unspecified",
        "anomaly_scenarios": _anomaly_scenarios(
            name=case_id,
            description=description,
            device_matrix={},
            payload=payload,
        ),
        **_BOUNDARY_FIELDS,
    }


def _merge_case_with_fixture(existing: dict[str, Any], fixture_case: dict[str, Any]) -> dict[str, Any]:
    merged = dict(existing)
    merged["source_kinds"] = sorted(
        set(str(item) for item in list(existing.get("source_kinds") or []) + list(fixture_case.get("source_kinds") or []))
    )
    merged["source_refs"] = {
        **dict(existing.get("source_refs") or {}),
        **dict(fixture_case.get("source_refs") or {}),
    }
    merged["temperature_points_c"] = _merge_number_lists(
        existing.get("temperature_points_c"),
        fixture_case.get("temperature_points_c"),
    )
    merged["temperature_point_categories"] = sorted(
        set(str(item) for item in list(existing.get("temperature_point_categories") or []))
        | set(str(item) for item in list(fixture_case.get("temperature_point_categories") or []))
    )
    merged["pressure_points_hpa"] = _merge_number_lists(
        existing.get("pressure_points_hpa"),
        fixture_case.get("pressure_points_hpa"),
    )
    merged["pressure_point_categories"] = sorted(
        set(str(item) for item in list(existing.get("pressure_point_categories") or []))
        | set(str(item) for item in list(fixture_case.get("pressure_point_categories") or []))
    )
    merged["gas_families"] = sorted(
        set(str(item) for item in list(existing.get("gas_families") or []))
        | set(str(item) for item in list(fixture_case.get("gas_families") or []))
    )
    merged["path_categories"] = sorted(
        set(str(item) for item in list(existing.get("path_categories") or []))
        | set(str(item) for item in list(fixture_case.get("path_categories") or []))
    )
    merged["anomaly_scenarios"] = sorted(
        set(str(item) for item in list(existing.get("anomaly_scenarios") or []))
        | set(str(item) for item in list(fixture_case.get("anomaly_scenarios") or []))
    )
    if not str(merged.get("validation_profile") or "").strip():
        merged["validation_profile"] = fixture_case.get("validation_profile")
    if not str(merged.get("description") or "").strip():
        merged["description"] = fixture_case.get("description")
    return merged


def _trace_metrics(payload: dict[str, Any]) -> dict[str, Any]:
    temperatures: set[float] = set()
    pressures: set[float] = set()
    gas_families: set[str] = set()
    environment_markers = False
    has_ambient = False
    target_routes: set[str] = set()

    for side_name in ("v1", "v2"):
        side = dict(payload.get(side_name) or {})
        for raw_line in list(side.get("trace_lines") or []):
            try:
                event = json.loads(str(raw_line))
            except Exception:
                continue
            if not isinstance(event, dict):
                continue
            route = str(event.get("route") or "").strip().lower()
            point_tag = str(event.get("point_tag") or "").strip().lower()
            action = str(event.get("action") or "").strip().lower()
            blob = " ".join([route, point_tag, action])
            if "h2o" in route or "h2o" in point_tag:
                gas_families.add("h2o")
                target_routes.add("h2o")
            if "co2" in route or "co2" in point_tag:
                gas_families.add("co2")
                target_routes.add("co2")
            if "ambient" in blob or "environment" in blob:
                environment_markers = True
                has_ambient = True
            if any(token in blob for token in ("thermometer", "pressure", "gauge", "humidity", "chamber")):
                environment_markers = True
            for match in _TEMP_PATTERN.finditer(point_tag):
                try:
                    temperatures.add(float(match.group("value")))
                except ValueError:
                    continue
            for match in _PRESSURE_PATTERN.finditer(point_tag):
                try:
                    pressures.add(float(match.group("value")))
                except ValueError:
                    continue
    target_route = ""
    if target_routes == {"co2", "h2o"}:
        target_route = "h2o_then_co2"
    elif target_routes:
        target_route = sorted(target_routes)[0]
    return {
        "temperatures_c": sorted(temperatures),
        "pressures_hpa": sorted(pressures),
        "gas_families": sorted(gas_families),
        "environment_markers": environment_markers,
        "has_ambient": has_ambient,
        "target_route": target_route,
    }


def _path_categories(*, name: str, description: str, target_route: str, route_mode: str) -> list[str]:
    categories = set()
    route_blob = " ".join([str(name or ""), str(description or ""), str(target_route or ""), str(route_mode or "")]).lower()
    if "h2o" in route_blob or "water" in route_blob:
        categories.add("water_path")
    if "co2" in route_blob or "gas" in route_blob:
        categories.add("gas_path")
    if any(
        token in route_blob
        for token in ("thermometer", "pressure", "gauge", "humidity", "temperature", "chamber", "ambient", "serial")
    ):
        categories.add("environment")
    return sorted(categories)


def _gas_families(target_route: str) -> list[str]:
    route = str(target_route or "").strip().lower()
    families = set()
    if "co2" in route:
        families.add("co2")
    if "h2o" in route:
        families.add("h2o")
    return sorted(families)


def _temperature_categories(values: list[float]) -> list[str]:
    categories = set()
    for value in values:
        if value <= 5.0:
            categories.add("zero_or_cold")
        elif value >= 35.0:
            categories.add("hot")
        else:
            categories.add("ambient")
    return sorted(categories)


def _pressure_categories(values: list[float], *, has_ambient: bool) -> list[str]:
    categories = set()
    if has_ambient:
        categories.add("ambient")
    for value in values:
        if value <= 700.0:
            categories.add("low_pressure")
        elif value >= 1050.0:
            categories.add("high_pressure")
        else:
            categories.add("nominal_pressure")
    return sorted(categories)


def _analyzer_population_category(device_matrix: dict[str, Any]) -> str:
    count = int(dict(device_matrix.get("analyzers") or {}).get("count") or 0)
    if count >= 8:
        return "fleet_8"
    if count >= 4:
        return "fleet_mid"
    if count >= 1:
        return "single_or_small_fleet"
    return "single_or_unspecified"


def _chain_length_category(device_matrix: dict[str, Any], target_route: str) -> str:
    if not device_matrix:
        return "unspecified"
    score = 0
    analyzers = dict(device_matrix.get("analyzers") or {})
    if int(analyzers.get("count") or 0) > 0:
        score += 1
    for device_name in ("pressure_controller", "pressure_gauge", "temperature_chamber"):
        if dict(device_matrix.get(device_name) or {}):
            score += 1
    for relay_name in ("relay", "relay_8"):
        relay = dict(device_matrix.get(relay_name) or {})
        if relay and not bool(relay.get("skipped_by_profile", False)):
            score += 1
    thermometer = dict(device_matrix.get("thermometer") or {})
    if thermometer and not bool(thermometer.get("skipped_by_profile", False)):
        score += 1
    if "h2o" in str(target_route or ""):
        humidity_generator = dict(device_matrix.get("humidity_generator") or {})
        dewpoint_meter = dict(device_matrix.get("dewpoint_meter") or {})
        if humidity_generator and not bool(humidity_generator.get("skipped_by_profile", False)):
            score += 1
        if dewpoint_meter and not bool(dewpoint_meter.get("skipped_by_profile", False)):
            score += 1
    if score >= 8:
        return "extended_chain"
    if score >= 6:
        return "full_chain"
    if score >= 4:
        return "medium_chain"
    return "short_chain"


def _anomaly_scenarios(
    *,
    name: str,
    description: str,
    device_matrix: dict[str, Any],
    payload: dict[str, Any],
) -> list[str]:
    text_parts = [
        str(name or ""),
        str(description or ""),
        " ".join(_scalar_text_values(device_matrix)),
        " ".join(_scalar_text_values(payload)),
    ]
    haystack = " ".join(text_parts).lower()
    anomalies = {
        anomaly
        for anomaly, keywords in _ANOMALY_KEYWORDS.items()
        if any(keyword in haystack for keyword in keywords)
    }
    if not anomalies:
        anomalies.add("nominal_success" if "success" in haystack else "coverage_case")
    return sorted(anomalies)


def _merge_number_lists(*values: Any) -> list[float]:
    merged: set[float] = set()
    for row in values:
        for item in list(row or []):
            try:
                merged.add(float(item))
            except (TypeError, ValueError):
                continue
    return sorted(merged)


def _scalar_text_values(payload: Any) -> list[str]:
    values: list[str] = []
    if isinstance(payload, dict):
        for value in payload.values():
            values.extend(_scalar_text_values(value))
        return values
    if isinstance(payload, list):
        for item in payload:
            values.extend(_scalar_text_values(item))
        return values
    text = str(payload or "").strip()
    return [text] if text else []


def _count_scalar_rows(rows: list[dict[str, Any]], key: str) -> Counter[str]:
    counter: Counter[str] = Counter()
    for row in rows:
        for item in list(row.get(key) or []):
            text = str(item or "").strip()
            if text:
                counter[text] += 1
    return counter


def _normalize_counter_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in payload.items():
        if isinstance(value, Counter):
            normalized[key] = dict(sorted(value.items()))
        else:
            normalized[key] = value
    return normalized
