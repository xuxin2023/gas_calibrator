from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Iterable, Optional

from ..core.offline_artifacts import (
    ANALYTICS_SUMMARY_FILENAME,
    export_run_offline_artifacts,
    export_suite_offline_artifacts,
    write_json,
)
from ..core.metrology_calibration_contract import (
    METROLOGY_CALIBRATION_CONTRACT_FILENAME,
    build_metrology_calibration_contract,
)
from ..core.phase_transition_bridge import (
    PHASE_TRANSITION_BRIDGE_FILENAME,
    build_phase_transition_bridge,
)
from ..core.phase_transition_bridge_presenter import build_phase_transition_bridge_panel_payload
from ..core.step2_readiness import (
    STEP2_READINESS_SUMMARY_FILENAME,
    build_step2_readiness_summary,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rebuild offline acceptance/analytics/lineage artifacts.")
    parser.add_argument("--run-dir", default=None, help="Run directory containing summary/manifest/results.")
    parser.add_argument("--suite-dir", default=None, help="Suite directory containing suite_summary.json.")
    return parser.parse_args(list(argv) if argv is not None else None)


def _load_json(path: Path) -> dict[str, object]:
    if not path.exists():
        raise FileNotFoundError(f"required artifact missing: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _objectify(value):
    if isinstance(value, dict):
        return SimpleNamespace(**{key: _objectify(item) for key, item in value.items()})
    if isinstance(value, list):
        return [_objectify(item) for item in value]
    return value


def _default_smoke_paths() -> tuple[Path, Path]:
    config_dir = Path(__file__).resolve().parents[1] / "configs"
    return config_dir / "smoke_v2_minimal.json", config_dir / "smoke_points_minimal.json"


def _augment_run_payload_with_step2_readiness(
    payload: dict[str, object],
    *,
    run_dir: Path,
    run_id: str,
    simulation_mode: bool,
) -> dict[str, object]:
    analytics_summary = dict(payload.get("summary_stats", {}).get("analytics_summary") or _load_json(run_dir / ANALYTICS_SUMMARY_FILENAME))
    smoke_config_path, smoke_points_path = _default_smoke_paths()
    readiness_summary = build_step2_readiness_summary(
        run_id=run_id,
        simulation_mode=simulation_mode,
        config_governance_handoff=dict(analytics_summary.get("config_governance_handoff") or {}),
        smoke_config_path=smoke_config_path,
        smoke_points_path=smoke_points_path,
    )
    analytics_summary["step2_readiness_summary"] = dict(readiness_summary)
    write_json(run_dir / ANALYTICS_SUMMARY_FILENAME, analytics_summary)
    readiness_path = write_json(run_dir / STEP2_READINESS_SUMMARY_FILENAME, readiness_summary)
    metrology_contract = build_metrology_calibration_contract(
        run_id=run_id,
        simulation_mode=simulation_mode,
        config_governance_handoff=dict(analytics_summary.get("config_governance_handoff") or {}),
    )
    analytics_summary["metrology_calibration_contract"] = dict(metrology_contract)
    write_json(run_dir / ANALYTICS_SUMMARY_FILENAME, analytics_summary)
    metrology_path = write_json(run_dir / METROLOGY_CALIBRATION_CONTRACT_FILENAME, metrology_contract)
    phase_transition_bridge = build_phase_transition_bridge(
        run_id=run_id,
        step2_readiness_summary=readiness_summary,
        metrology_calibration_contract=metrology_contract,
    )
    phase_transition_bridge_surface_bundle = build_phase_transition_bridge_panel_payload(phase_transition_bridge)
    analytics_summary["phase_transition_bridge"] = dict(phase_transition_bridge)
    analytics_summary["phase_transition_bridge_reviewer_section"] = dict(phase_transition_bridge_surface_bundle)
    write_json(run_dir / ANALYTICS_SUMMARY_FILENAME, analytics_summary)
    phase_transition_path = write_json(run_dir / PHASE_TRANSITION_BRIDGE_FILENAME, phase_transition_bridge)

    summary_stats = dict(payload.get("summary_stats") or {})
    summary_stats["analytics_summary"] = analytics_summary
    summary_stats["step2_readiness_summary"] = dict(readiness_summary)
    summary_stats["metrology_calibration_contract"] = dict(metrology_contract)
    summary_stats["phase_transition_bridge"] = dict(phase_transition_bridge)
    summary_stats["step2_readiness_digest"] = {
        "phase": readiness_summary.get("phase"),
        "overall_status": readiness_summary.get("overall_status"),
        "ready_for_engineering_isolation": bool(readiness_summary.get("ready_for_engineering_isolation", False)),
        "real_acceptance_ready": bool(readiness_summary.get("real_acceptance_ready", False)),
        "gate_status_counts": dict(readiness_summary.get("gate_status_counts") or {}),
        "blocking_items": list(readiness_summary.get("blocking_items") or []),
        "warning_items": list(readiness_summary.get("warning_items") or []),
        "evidence_mode": readiness_summary.get("evidence_mode"),
    }
    summary_stats["metrology_calibration_contract_digest"] = {
        "phase": metrology_contract.get("phase"),
        "overall_status": metrology_contract.get("overall_status"),
        "real_acceptance_ready": bool(metrology_contract.get("real_acceptance_ready", False)),
        "stage_assignment": dict(metrology_contract.get("stage_assignment") or {}),
        "stage3_execution_items": list(metrology_contract.get("stage3_execution_items") or []),
        "blocking_items": list(metrology_contract.get("blocking_items") or []),
        "warning_items": list(metrology_contract.get("warning_items") or []),
        "evidence_mode": metrology_contract.get("evidence_mode"),
    }
    summary_stats["phase_transition_bridge_digest"] = {
        "phase": phase_transition_bridge.get("phase"),
        "overall_status": phase_transition_bridge.get("overall_status"),
        "recommended_next_stage": phase_transition_bridge.get("recommended_next_stage"),
        "ready_for_engineering_isolation": bool(phase_transition_bridge.get("ready_for_engineering_isolation", False)),
        "real_acceptance_ready": bool(phase_transition_bridge.get("real_acceptance_ready", False)),
        "blocking_items": list(phase_transition_bridge.get("blocking_items") or []),
        "warning_items": list(phase_transition_bridge.get("warning_items") or []),
        "missing_real_world_evidence": list(phase_transition_bridge.get("missing_real_world_evidence") or []),
    }
    summary_stats["phase_transition_bridge_reviewer_section"] = dict(phase_transition_bridge_surface_bundle)
    payload["summary_stats"] = summary_stats

    artifact_statuses = dict(payload.get("artifact_statuses") or {})
    artifact_statuses["step2_readiness_summary"] = {
        "status": "ok",
        "role": "execution_summary",
        "path": str(readiness_path),
    }
    artifact_statuses["metrology_calibration_contract"] = {
        "status": "ok",
        "role": "formal_analysis",
        "path": str(metrology_path),
    }
    artifact_statuses["phase_transition_bridge"] = {
        "status": "ok",
        "role": "execution_summary",
        "path": str(phase_transition_path),
    }
    payload["artifact_statuses"] = artifact_statuses

    manifest_sections = dict(payload.get("manifest_sections") or {})
    manifest_sections["step2_readiness"] = {
        "phase": readiness_summary.get("phase"),
        "overall_status": readiness_summary.get("overall_status"),
        "ready_for_engineering_isolation": bool(readiness_summary.get("ready_for_engineering_isolation", False)),
        "real_acceptance_ready": bool(readiness_summary.get("real_acceptance_ready", False)),
        "evidence_mode": readiness_summary.get("evidence_mode"),
        "blocking_items": list(readiness_summary.get("blocking_items") or []),
        "warning_items": list(readiness_summary.get("warning_items") or []),
        "gate_status_counts": dict(readiness_summary.get("gate_status_counts") or {}),
        "not_real_acceptance_evidence": bool(readiness_summary.get("not_real_acceptance_evidence", True)),
    }
    manifest_sections["metrology_calibration_contract"] = {
        "phase": metrology_contract.get("phase"),
        "overall_status": metrology_contract.get("overall_status"),
        "real_acceptance_ready": bool(metrology_contract.get("real_acceptance_ready", False)),
        "stage_assignment": dict(metrology_contract.get("stage_assignment") or {}),
        "stage3_execution_items": list(metrology_contract.get("stage3_execution_items") or []),
        "blocking_items": list(metrology_contract.get("blocking_items") or []),
        "warning_items": list(metrology_contract.get("warning_items") or []),
        "not_real_acceptance_evidence": bool(metrology_contract.get("not_real_acceptance_evidence", True)),
    }
    manifest_sections["phase_transition_bridge"] = {
        "phase": phase_transition_bridge.get("phase"),
        "overall_status": phase_transition_bridge.get("overall_status"),
        "recommended_next_stage": phase_transition_bridge.get("recommended_next_stage"),
        "ready_for_engineering_isolation": bool(phase_transition_bridge.get("ready_for_engineering_isolation", False)),
        "real_acceptance_ready": bool(phase_transition_bridge.get("real_acceptance_ready", False)),
        "blocking_items": list(phase_transition_bridge.get("blocking_items") or []),
        "warning_items": list(phase_transition_bridge.get("warning_items") or []),
        "not_real_acceptance_evidence": True,
    }
    manifest_sections["phase_transition_bridge_reviewer_section"] = dict(phase_transition_bridge_surface_bundle)
    payload["manifest_sections"] = manifest_sections

    remembered_files = [str(item) for item in list(payload.get("remembered_files") or [])]
    readiness_path_text = str(readiness_path)
    if readiness_path_text not in remembered_files:
        remembered_files.append(readiness_path_text)
    metrology_path_text = str(metrology_path)
    if metrology_path_text not in remembered_files:
        remembered_files.append(metrology_path_text)
    phase_transition_path_text = str(phase_transition_path)
    if phase_transition_path_text not in remembered_files:
        remembered_files.append(phase_transition_path_text)
    payload["remembered_files"] = remembered_files
    return payload


def rebuild_run(run_dir: Path) -> dict[str, object]:
    for name in ("summary.json", "manifest.json", "results.json"):
        if not (run_dir / name).exists():
            raise FileNotFoundError(
                f"{run_dir} is not a formal V2 run directory. Missing {name}. "
                "Use a run directory that contains summary.json, manifest.json, and results.json."
            )
    summary = _load_json(run_dir / "summary.json")
    manifest = _load_json(run_dir / "manifest.json")
    results = _load_json(run_dir / "results.json")
    session = SimpleNamespace(
        run_id=str(summary.get("run_id") or manifest.get("run_id") or run_dir.name),
        config=_objectify(dict(manifest.get("config_snapshot") or {})),
    )
    payload = export_run_offline_artifacts(
        run_dir=run_dir,
        output_dir=run_dir.parent,
        run_id=str(session.run_id),
        session=session,
        samples=[_objectify(item) for item in list(results.get("samples") or [])],
        point_summaries=[dict(item) for item in list(results.get("point_summaries") or [])],
        output_files=list((summary.get("stats") or {}).get("output_files") or []),
        export_statuses=dict((summary.get("stats") or {}).get("artifact_exports") or {}),
        source_points_file=manifest.get("source_points_file"),
        software_build_id=str(manifest.get("software_build_id") or summary.get("software_build_id") or ""),
        config_safety=dict(summary.get("config_safety") or (summary.get("stats") or {}).get("config_safety") or {}),
        config_safety_review=dict(
            summary.get("config_safety_review") or (summary.get("stats") or {}).get("config_safety_review") or {}
        ),
    )
    simulation_mode = bool(getattr(getattr(session.config, "features", None), "simulation_mode", False))
    return _augment_run_payload_with_step2_readiness(
        payload,
        run_dir=run_dir,
        run_id=str(session.run_id),
        simulation_mode=simulation_mode,
    )


def rebuild_suite(suite_dir: Path) -> dict[str, object]:
    if not (suite_dir / "suite_summary.json").exists():
        raise FileNotFoundError(
            f"{suite_dir} is not a suite directory. Missing suite_summary.json."
        )
    summary = _load_json(suite_dir / "suite_summary.json")
    return export_suite_offline_artifacts(suite_dir=suite_dir, summary=summary)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    if bool(args.run_dir) == bool(args.suite_dir):
        print("Provide exactly one of --run-dir or --suite-dir.", file=sys.stderr)
        return 2
    try:
        if args.run_dir:
            payload = rebuild_run(Path(str(args.run_dir)).resolve())
            print(f"acceptance_plan: {Path(args.run_dir).resolve() / 'acceptance_plan.json'}")
            print(f"analytics_summary: {Path(args.run_dir).resolve() / 'analytics_summary.json'}")
            print(f"step2_readiness_summary: {Path(args.run_dir).resolve() / STEP2_READINESS_SUMMARY_FILENAME}")
            print(f"metrology_calibration_contract: {Path(args.run_dir).resolve() / METROLOGY_CALIBRATION_CONTRACT_FILENAME}")
            print(f"phase_transition_bridge: {Path(args.run_dir).resolve() / PHASE_TRANSITION_BRIDGE_FILENAME}")
            print(f"lineage_summary: {Path(args.run_dir).resolve() / 'lineage_summary.json'}")
            print(f"trend_registry: {Path(args.run_dir).resolve() / 'trend_registry.json'}")
            print(f"evidence_registry: {Path(args.run_dir).resolve() / 'evidence_registry.json'}")
            print(f"coefficient_registry: {Path(args.run_dir).resolve() / 'coefficient_registry.json'}")
            return 0 if payload else 1
        payload = rebuild_suite(Path(str(args.suite_dir)).resolve())
        print(f"suite_analytics_summary: {Path(args.suite_dir).resolve() / 'suite_analytics_summary.json'}")
        print(f"suite_acceptance_plan: {Path(args.suite_dir).resolve() / 'suite_acceptance_plan.json'}")
        print(f"suite_evidence_registry: {Path(args.suite_dir).resolve() / 'suite_evidence_registry.json'}")
        return 0 if payload else 1
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
