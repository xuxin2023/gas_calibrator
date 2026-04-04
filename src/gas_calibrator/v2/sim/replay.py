from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional

from ..scripts import compare_v1_v2_control_flow


PROJECT_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_REPLAY_FIXTURE_ROOT = PROJECT_ROOT / "tests" / "v2" / "fixtures" / "replay"
REPLAY_SCENARIO_ALIASES = {
    "stale_h2o_latest_present_but_not_primary": "stale_h2o_latest_present_but_must_not_be_primary",
}


def list_replay_scenarios(root: Optional[Path] = None) -> list[str]:
    fixture_root = Path(root or DEFAULT_REPLAY_FIXTURE_ROOT)
    if not fixture_root.exists():
        return []
    return sorted(path.stem for path in fixture_root.glob("*.json"))


def load_replay_fixture(
    *,
    scenario: Optional[str] = None,
    fixture: Optional[str] = None,
    root: Optional[Path] = None,
) -> dict[str, Any]:
    fixture_root = Path(root or DEFAULT_REPLAY_FIXTURE_ROOT)
    if fixture:
        path = Path(fixture)
    elif scenario:
        scenario_name = REPLAY_SCENARIO_ALIASES.get(str(scenario), str(scenario))
        path = fixture_root / f"{scenario_name}.json"
    else:
        raise ValueError("scenario or fixture is required")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"invalid replay fixture: {path}")
    payload.setdefault("scenario", path.stem)
    payload["_fixture_path"] = str(path)
    return payload


def _write_trace(path: Path, lines: list[str]) -> Optional[Path]:
    if not lines:
        return None
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(str(line) for line in lines) + "\n", encoding="utf-8")
    return path


def _side_metadata(
    side_payload: dict[str, Any],
    *,
    trace_path: Path,
    runtime_policy: dict[str, Any],
    config_label: str,
) -> dict[str, Any]:
    payload = {
        "config_path": config_label,
        "ok": bool(side_payload.get("ok", False)),
        "exit_code": int(side_payload.get("exit_code", 0 if side_payload.get("ok", False) else 1)),
        "run_id": str(side_payload.get("run_id") or ""),
        "run_dir": str(side_payload.get("run_dir") or trace_path.parent),
        "trace_path": str(trace_path),
        "runtime_config_path": str(side_payload.get("runtime_config_path") or ""),
        "status_phase": side_payload.get("status_phase"),
        "status_error": side_payload.get("status_error"),
        "error_category": side_payload.get("error_category"),
        "derived_failure_phase": side_payload.get("derived_failure_phase"),
        "last_runner_stage": side_payload.get("last_runner_stage"),
        "last_runner_event": side_payload.get("last_runner_event"),
        "abort_message": side_payload.get("abort_message"),
        "trace_expected_but_missing": side_payload.get("trace_expected_but_missing"),
        "cleanup_terminated": side_payload.get("cleanup_terminated"),
        "cleanup_termination_reason": side_payload.get("cleanup_termination_reason"),
        "runtime_policy": runtime_policy,
        "preflight": dict(side_payload.get("preflight") or {}),
    }
    return payload


def _default_bench_context(validation_profile: str) -> dict[str, Any]:
    return compare_v1_v2_control_flow._validation_bench_context(validation_profile)


def _materialize_compare_fixture(
    payload: dict[str, Any],
    *,
    report_root: Path,
    run_name: Optional[str],
    publish_latest: bool,
    evidence_source: str,
    validation_profile_override: Optional[str],
    simulation_context_override: Optional[dict[str, Any]],
    evidence_state_override: Optional[str],
) -> dict[str, Any]:
    validation_profile = str(
        validation_profile_override
        or payload.get("validation_profile")
        or compare_v1_v2_control_flow.SKIP0_CO2_ONLY_VALIDATION_PROFILE
    )
    scenario_name = str(payload.get("scenario") or "replay_case")
    report_dir = report_root / str(run_name or scenario_name)
    report_dir.mkdir(parents=True, exist_ok=True)

    v1_side = dict(payload.get("v1") or {})
    v2_side = dict(payload.get("v2") or {})
    v1_trace_path = report_dir / "fixture_v1_route_trace.jsonl"
    v2_trace_path = report_dir / "fixture_v2_route_trace.jsonl"
    wrote_v1 = _write_trace(v1_trace_path, list(v1_side.get("trace_lines") or []))
    wrote_v2 = _write_trace(v2_trace_path, list(v2_side.get("trace_lines") or []))
    if wrote_v1 is None:
        v1_trace_path = report_dir / "fixture_v1_route_trace_missing.jsonl"
    if wrote_v2 is None:
        v2_trace_path = report_dir / "fixture_v2_route_trace_missing.jsonl"

    route_mode = str(payload.get("route_mode") or "co2_only")
    bench_context = dict(payload.get("bench_context") or _default_bench_context(validation_profile))
    simulation_context = dict(payload.get("simulation_context") or {})
    if simulation_context_override:
        simulation_context.update(dict(simulation_context_override))
    effective_validation_mode = dict(payload.get("effective_validation_mode") or {})
    target_route = (
        effective_validation_mode.get("target_route")
        or compare_v1_v2_control_flow._target_route_for_compare(validation_profile=validation_profile, route_mode=route_mode)
    )
    effective_validation_mode.setdefault("validation_profile", validation_profile)
    effective_validation_mode.setdefault("route_mode", route_mode)
    effective_validation_mode.setdefault("target_route", target_route)
    effective_validation_mode.setdefault("diagnostic_only", bool(bench_context.get("diagnostic_only", False)))
    effective_validation_mode.setdefault("acceptance_evidence", bool(bench_context.get("acceptance_evidence", False)))

    v1_runtime_policy = dict(payload.get("v1_runtime_policy") or v1_side.get("runtime_policy") or {})
    v2_runtime_policy = dict(payload.get("v2_runtime_policy") or v2_side.get("runtime_policy") or {})
    preflight = dict(payload.get("preflight") or {})
    preflight.setdefault("ok", True)
    preflight.setdefault("status", "ok")
    preflight.setdefault("reason", "")
    sides_preflight = preflight.setdefault("sides", {})
    if not isinstance(sides_preflight, dict):
        sides_preflight = {}
        preflight["sides"] = sides_preflight
    sides_preflight.setdefault("v1", dict(v1_side.get("preflight") or {"side": "v1", "status": "ok", "ok": True}))
    sides_preflight.setdefault("v2", dict(v2_side.get("preflight") or {"side": "v2", "status": "ok", "ok": True}))

    metadata = {
        "run_name": str(run_name or scenario_name),
        "validation_profile": validation_profile,
        "evidence_source": evidence_source,
        "temp_c": payload.get("temp_c"),
        "skip_co2_ppm": list(payload.get("skip_co2_ppm") or []),
        "route_mode": route_mode,
        "skip_connect_check": bool(payload.get("skip_connect_check", True)),
        "simulation": True,
        "preflight": preflight,
        "bench_context": bench_context,
        "simulation_context": simulation_context,
        "effective_v2_compare_config": str(payload.get("effective_v2_compare_config") or f"fixture:{scenario_name}"),
        "effective_validation_mode": effective_validation_mode,
        "evidence_state_override": evidence_state_override,
        "runtime_policies": {
            "v1": v1_runtime_policy,
            "v2": v2_runtime_policy,
        },
        "v1": _side_metadata(
            v1_side,
            trace_path=v1_trace_path,
            runtime_policy=v1_runtime_policy,
            config_label=f"fixture:{scenario_name}:v1",
        ),
        "v2": _side_metadata(
            v2_side,
            trace_path=v2_trace_path,
            runtime_policy=v2_runtime_policy,
            config_label=f"fixture:{scenario_name}:v2",
        ),
    }
    report = compare_v1_v2_control_flow.build_control_flow_report(
        v1_trace_path=v1_trace_path,
        v2_trace_path=v2_trace_path,
        metadata=metadata,
    )
    report["artifacts"] = {
        **compare_v1_v2_control_flow._write_trace_artifact_copies(
            report_dir=report_dir,
            v1_trace_path=v1_trace_path,
            v2_trace_path=v2_trace_path,
        ),
        **compare_v1_v2_control_flow._write_compare_side_artifacts(
            report_dir=report_dir,
            report=report,
            v1_trace_path=v1_trace_path,
            v2_trace_path=v2_trace_path,
        ),
        "control_flow_compare_report_json": str(report_dir / "control_flow_compare_report.json"),
        "control_flow_compare_report_markdown": str(report_dir / "control_flow_compare_report.md"),
    }
    json_path = compare_v1_v2_control_flow._write_json(report_dir / "control_flow_compare_report.json", report)
    markdown_path = compare_v1_v2_control_flow._write_text(
        report_dir / "control_flow_compare_report.md",
        compare_v1_v2_control_flow.format_control_flow_report_markdown(report),
    )
    report["artifact_inventory"] = compare_v1_v2_control_flow.build_artifact_inventory(report.get("artifacts") or {})
    artifact_inventory_path = compare_v1_v2_control_flow._write_json(
        report_dir / "artifact_inventory.json",
        report["artifact_inventory"],
    )
    report["artifacts"]["artifact_inventory"] = str(artifact_inventory_path)
    report["artifacts"].update(
        compare_v1_v2_control_flow._write_replacement_validation_indexes(
            report_root=report_root,
            report_dir=report_dir,
            report=report,
            update_latest=publish_latest,
        )
    )
    compare_v1_v2_control_flow._write_json(report_dir / "control_flow_compare_report.json", report)
    compare_v1_v2_control_flow._write_text(
        report_dir / "control_flow_compare_report.md",
        compare_v1_v2_control_flow.format_control_flow_report_markdown(report),
    )
    return {
        "scenario": scenario_name,
        "report_dir": str(report_dir),
        "report_json": str(json_path),
        "report_markdown": str(markdown_path),
        "artifact_inventory": str(artifact_inventory_path),
        "latest_indexes": {
            key: value
            for key, value in (report.get("artifacts") or {}).items()
            if key.endswith("_latest")
        },
        "compare_status": report.get("compare_status"),
        "report": report,
    }


def _materialize_snapshot_fixture(
    payload: dict[str, Any],
    *,
    report_root: Path,
) -> dict[str, Any]:
    written: list[str] = []
    for item in list(payload.get("latest_indexes") or []):
        filename = str(item.get("filename") or "").strip()
        latest_payload = item.get("payload")
        if not filename or not isinstance(latest_payload, dict):
            continue
        written.append(str(compare_v1_v2_control_flow._write_json(report_root / filename, latest_payload)))
    return {
        "scenario": str(payload.get("scenario") or "validation_snapshot"),
        "report_dir": str(report_root),
        "latest_indexes": written,
        "compare_status": str(payload.get("compare_status") or "SNAPSHOT_ONLY"),
    }


def materialize_replay_fixture(
    payload: dict[str, Any],
    *,
    report_root: Path,
    run_name: Optional[str] = None,
    publish_latest: bool = False,
    evidence_source: str = compare_v1_v2_control_flow.EVIDENCE_SOURCE_SIMULATED,
    validation_profile_override: Optional[str] = None,
    simulation_context_override: Optional[dict[str, Any]] = None,
    evidence_state_override: Optional[str] = None,
) -> dict[str, Any]:
    kind = str(payload.get("kind") or "compare").strip().lower()
    report_root.mkdir(parents=True, exist_ok=True)
    if kind == "validation_snapshot":
        return _materialize_snapshot_fixture(payload, report_root=report_root)
    return _materialize_compare_fixture(
        payload,
        report_root=report_root,
        run_name=run_name,
        publish_latest=publish_latest,
        evidence_source=evidence_source,
        validation_profile_override=validation_profile_override,
        simulation_context_override=simulation_context_override,
        evidence_state_override=evidence_state_override,
    )
