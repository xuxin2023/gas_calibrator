from __future__ import annotations

import copy
import json
import shutil
from pathlib import Path
from typing import Any, Optional

from .replay import DEFAULT_REPLAY_FIXTURE_ROOT, load_replay_fixture, materialize_replay_fixture
from .scenarios import get_simulated_scenario, simulated_profile_defaults


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in dict(override or {}).items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(dict(merged[key]), value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _load_protocol_profile_config(profile: str) -> tuple[Path, dict[str, Any]]:
    from ..scripts import compare_v1_v2_control_flow as compare

    config_path = compare._validation_config_for_profile(profile)
    if config_path is None:
        raise ValueError(f"unknown simulated validation profile: {profile}")
    resolved, raw_cfg, _ = compare.load_config_bundle(str(config_path), simulation_mode=True)
    return Path(resolved), raw_cfg


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def _protocol_v1_baseline(
    *,
    report_dir: Path,
    scenario_name: str,
    baseline_mode: str,
    v2_run: dict[str, Any],
) -> dict[str, Any]:
    if baseline_mode == "mirror_v2":
        source_trace = Path(str(v2_run.get("trace_path") or ""))
        trace_path = report_dir / "simulated_v1_route_trace.jsonl"
        if source_trace.exists():
            shutil.copyfile(source_trace, trace_path)
        return {
            "ok": True,
            "exit_code": 0,
            "run_id": f"{scenario_name}_simulated_v1",
            "run_dir": str(trace_path.parent),
            "trace_path": str(trace_path),
            "runtime_config_path": "",
            "status_phase": "simulated.protocol_baseline",
            "status_error": None,
        }

    fixture = load_replay_fixture(scenario=scenario_name, root=DEFAULT_REPLAY_FIXTURE_ROOT)
    v1_side = dict(fixture.get("v1") or {})
    trace_lines = list(v1_side.get("trace_lines") or [])
    trace_path = report_dir / "fixture_v1_route_trace.jsonl"
    if trace_lines:
        trace_path.write_text("\n".join(trace_lines) + "\n", encoding="utf-8")
    return {
        "ok": bool(v1_side.get("ok", True)),
        "exit_code": int(v1_side.get("exit_code", 0 if v1_side.get("ok", True) else 1)),
        "run_id": str(v1_side.get("run_id") or f"{scenario_name}_fixture_v1"),
        "run_dir": str(v1_side.get("run_dir") or trace_path.parent),
        "trace_path": str(trace_path),
        "runtime_config_path": str(v1_side.get("runtime_config_path") or ""),
        "status_phase": v1_side.get("status_phase"),
        "status_error": v1_side.get("status_error"),
        "error_category": v1_side.get("error_category"),
        "derived_failure_phase": v1_side.get("derived_failure_phase"),
    }


def build_protocol_simulated_compare_result(
    *,
    profile: str,
    scenario: Optional[str],
    report_root: Path,
    run_name: Optional[str],
    publish_latest: bool,
) -> dict[str, Any]:
    from ..scripts import compare_v1_v2_control_flow as compare

    defaults = simulated_profile_defaults(profile)
    scenario_name = str(scenario or defaults["scenario"])
    scenario_def = get_simulated_scenario(scenario_name)
    if str(scenario_def.execution_mode or "fixture").strip().lower() != "protocol":
        fixture = load_replay_fixture(scenario=scenario_def.fixture_name, root=DEFAULT_REPLAY_FIXTURE_ROOT)
        return materialize_replay_fixture(
            fixture,
            report_root=report_root,
            run_name=run_name,
            publish_latest=publish_latest,
            validation_profile_override=profile,
            simulation_context_override=scenario_def.simulation_context(),
            evidence_state_override="simulated_fixture",
        )

    config_path, raw_cfg = _load_protocol_profile_config(profile)
    report_dir = report_root / str(run_name or scenario_name)
    report_dir.mkdir(parents=True, exist_ok=True)
    runtime_cfg = _deep_merge(raw_cfg, dict(scenario_def.runtime_overrides or {}))
    simulation_context = scenario_def.simulation_context()
    simulation_context.setdefault(
        "protocol_devices",
        {
            "analyzer": "ygas",
            "pressure_controller": "pace_scpi",
            "humidity_generator": "grz5013",
            "temperature_chamber": "modbus",
            "relay": "modbus_rtu",
            "relay_8": "modbus_rtu",
            "thermometer": "ascii_stream",
        },
    )
    runtime_cfg["simulation_context"] = simulation_context
    runtime_cfg.setdefault("features", {})["simulation_mode"] = True
    runtime_cfg.setdefault("paths", {})["output_dir"] = str((report_dir / "v2_output").resolve())
    runtime_cfg_path = _write_json(report_dir / "runtime_v2_config.json", runtime_cfg)
    v2_run = compare._run_v2_trace(runtime_cfg_path, simulation_mode=True)
    v1_run = _protocol_v1_baseline(
        report_dir=report_dir,
        scenario_name=scenario_name,
        baseline_mode=str(scenario_def.baseline_mode or "mirror_v2"),
        v2_run=v2_run,
    )
    bench_context = compare._validation_bench_context(profile)
    effective_target_route = compare._target_route_for_compare(
        validation_profile=profile,
        route_mode=str(runtime_cfg.get("workflow", {}).get("route_mode") or ""),
    )
    report = compare.build_control_flow_report(
        v1_trace_path=Path(str(v1_run["trace_path"])),
        v2_trace_path=Path(str(v2_run["trace_path"])),
        metadata={
            "run_name": str(run_name or scenario_name),
            "validation_profile": profile,
            "evidence_source": compare.EVIDENCE_SOURCE_SIMULATED,
            "evidence_state_override": "simulated_protocol",
            "temp_c": None,
            "skip_co2_ppm": list(runtime_cfg.get("workflow", {}).get("skip_co2_ppm") or []),
            "route_mode": runtime_cfg.get("workflow", {}).get("route_mode"),
            "skip_connect_check": False,
            "simulation": True,
            "preflight": {"ok": True, "status": "ok", "reason": "", "sides": {"v1": {"ok": True}, "v2": {"ok": True}}},
            "bench_context": bench_context,
            "simulation_context": simulation_context,
            "effective_v2_compare_config": str(config_path),
            "effective_validation_mode": {
                "validation_profile": profile,
                "route_mode": runtime_cfg.get("workflow", {}).get("route_mode"),
                "target_route": effective_target_route,
                "diagnostic_only": bool(simulation_context.get("diagnostic_only", False)),
                "acceptance_evidence": False,
            },
            "runtime_policies": {
                "v1": {"simulation_baseline": str(scenario_def.baseline_mode or "mirror_v2")},
                "v2": compare._runtime_policy_summary(runtime_cfg, effective_compare_config=config_path, bench_context=bench_context),
            },
            "v1": {
                "config_path": f"simulated:{scenario_name}:v1_baseline",
                **v1_run,
            },
            "v2": {
                "config_path": str(config_path),
                **v2_run,
            },
        },
    )
    report["artifacts"] = {
        **compare._write_trace_artifact_copies(
            report_dir=report_dir,
            v1_trace_path=Path(str(v1_run["trace_path"])),
            v2_trace_path=Path(str(v2_run["trace_path"])),
        ),
        **compare._write_compare_side_artifacts(
            report_dir=report_dir,
            report=report,
            v1_trace_path=Path(str(v1_run["trace_path"])),
            v2_trace_path=Path(str(v2_run["trace_path"])),
        ),
        "control_flow_compare_report_json": str(report_dir / "control_flow_compare_report.json"),
        "control_flow_compare_report_markdown": str(report_dir / "control_flow_compare_report.md"),
        "runtime_v2_config": str(runtime_cfg_path),
    }
    json_path = compare._write_json(report_dir / "control_flow_compare_report.json", report)
    markdown_path = compare._write_text(
        report_dir / "control_flow_compare_report.md",
        compare.format_control_flow_report_markdown(report),
    )
    report["artifact_inventory"] = compare.build_artifact_inventory(report.get("artifacts") or {})
    artifact_inventory_path = compare._write_json(report_dir / "artifact_inventory.json", report["artifact_inventory"])
    report["artifacts"]["artifact_inventory"] = str(artifact_inventory_path)
    report["artifacts"].update(
        compare._write_replacement_validation_indexes(
            report_root=report_root,
            report_dir=report_dir,
            report=report,
            update_latest=publish_latest,
        )
    )
    compare._write_json(report_dir / "control_flow_compare_report.json", report)
    compare._write_text(
        report_dir / "control_flow_compare_report.md",
        compare.format_control_flow_report_markdown(report),
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
            if str(key).endswith("_latest")
        },
        "compare_status": report.get("compare_status"),
        "report": report,
    }
