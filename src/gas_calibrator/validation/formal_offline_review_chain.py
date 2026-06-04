"""One-step offline review chain for V1.5 formal no-write evidence.

The chain coordinates existing sidecar tools. It reads and writes files only:
no COM ports, no water/gas route control, no PACE/valve control, and no analyzer
coefficient writes.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

from ..storage.v1_5_evidence.bundle import sha256_file
from .formal_evidence_run import run_formal_evidence_sidecar
from .formal_readiness import write_formal_readiness_report
from .formal_reports import write_v1_5_calibration_reports
from .formal_workbench import write_formal_workbench
from ..v1_5.qc_advanced.exporter import write_advanced_qc_summary
from ..v1_5.review_surface import load_json_object, write_review_surface
from ..v1_5.ui.operation_console import write_operation_console


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _load_json(path: str | Path) -> Dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON object expected: {path}")
    return payload


def _path_or_empty(path: Any) -> str:
    return str(path) if path else ""


def _check_status(model: Mapping[str, Any], check: str) -> str:
    for row in model.get("checks") or []:
        if row.get("check") == check:
            return str(row.get("status") or "")
    return ""


def _output_artifact_manifest(outputs: Mapping[str, str], *, exclude_keys: set[str] | None = None) -> Dict[str, Any]:
    excluded = set(exclude_keys or set())
    artifacts = []
    for key, value in sorted(outputs.items()):
        if key in excluded or not value:
            continue
        path = Path(value)
        exists = path.exists()
        is_file = path.is_file()
        row: Dict[str, Any] = {
            "output_key": key,
            "artifact_role": key,
            "path": str(path),
            "exists": exists,
            "is_file": is_file,
            "sha256": sha256_file(path) if is_file else "",
            "size_bytes": path.stat().st_size if is_file else None,
        }
        artifacts.append(row)
    missing = [row for row in artifacts if not row["exists"] or not row["is_file"]]
    unhashed = [row for row in artifacts if row["is_file"] and not row["sha256"]]
    return {
        "schema": "v1_5_formal_offline_review_chain_artifacts_v1",
        "generated_at": _now(),
        "status": "pass" if not missing and not unhashed else "fail",
        "artifact_count": len(artifacts),
        "missing_count": len(missing),
        "unhashed_count": len(unhashed),
        "artifacts": artifacts,
        "physical_boundaries": {
            "sidecar_only": True,
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "controls_valves_or_pace": False,
            "writes_coefficients": False,
            "not_real_acceptance_evidence": True,
        },
    }


def _full_sidecar_allowed(readiness: Mapping[str, Any]) -> bool:
    required = {
        "formal_plan_contract",
        "pressure_reference_contract",
        "no_write_config",
        "pressure_quick_check_contract",
        "open_flow_samples",
    }
    return all(_check_status(readiness, item) == "pass" for item in required)


def render_offline_review_chain_markdown(summary: Mapping[str, Any]) -> str:
    lines = [
        "# V1.5 formal offline review chain",
        "",
        f"- chain_status: {summary.get('chain_status')}",
        f"- readiness_status: {summary.get('readiness_status')}",
        f"- output_dir: {summary.get('output_dir')}",
        f"- sidecar_only: {summary.get('sidecar_only')}",
        f"- opens_com_ports: {summary.get('opens_com_ports')}",
        f"- controls_water_or_gas_routes: {summary.get('controls_water_or_gas_routes')}",
        f"- controls_valves_or_pace: {summary.get('controls_valves_or_pace')}",
        f"- writes_coefficients: {summary.get('writes_coefficients')}",
        "",
        "## Stages",
    ]
    for stage, item in (summary.get("stages") or {}).items():
        lines.append(f"- {stage}: {item.get('status')} {item.get('reason', '')}".rstrip())
    lines.extend(["", "## Outputs"])
    for name, path in (summary.get("outputs") or {}).items():
        if path:
            lines.append(f"- {name}: {path}")
    return "\n".join(lines) + "\n"


def run_formal_offline_review_chain(
    *,
    run_dir: str | Path,
    plan_path: str | Path,
    pressure_reference_path: str | Path,
    config_path: str | Path | None = None,
    output_dir: str | Path | None = None,
    component: str = "both",
    analyzer_prefix: str = "ga01",
    reviewer: str = "",
    approver: str = "",
    location: str = "",
    calibration_date: str = "",
    uncertainty_json: str | Path | None = None,
    role: str = "operator",
    today: Any = None,
) -> Dict[str, Any]:
    root = Path(run_dir).resolve()
    destination = Path(output_dir).resolve() if output_dir else root / "formal_offline_review_chain"
    destination.mkdir(parents=True, exist_ok=True)

    outputs: Dict[str, str] = {}
    stages: Dict[str, Dict[str, Any]] = {}

    readiness_outputs = write_formal_readiness_report(
        run_dir=root,
        plan_path=plan_path,
        pressure_reference_path=pressure_reference_path,
        config_path=config_path,
        output_dir=destination / "readiness",
        component=component,
        analyzer_prefix=analyzer_prefix,
        today=today,
    )
    readiness_json = readiness_outputs["summary_json"]
    readiness = _load_json(readiness_json)
    outputs["readiness_json"] = str(readiness_json)
    outputs["readiness_markdown"] = str(readiness_outputs["summary_markdown"])
    outputs["readiness_workbook"] = str(readiness_outputs["workbook"])
    stages["readiness"] = {"status": "completed", "reason": readiness.get("readiness_status", "")}

    sidecar_output_dir = destination / "sidecar"
    sidecar_stage = "all" if _full_sidecar_allowed(readiness) else "preflight"
    try:
        sidecar_summary = run_formal_evidence_sidecar(
            run_dir=root,
            plan_path=plan_path,
            pressure_reference_path=pressure_reference_path,
            config_path=config_path,
            output_dir=sidecar_output_dir,
            stage=sidecar_stage,
            component=component,
            analyzer_prefix=analyzer_prefix,
            require_quick_check_artifact=True,
            today=today,
        )
        sidecar_summary_path = sidecar_output_dir / "formal_evidence_sidecar_summary.json"
        outputs["sidecar_summary_json"] = str(sidecar_summary_path)
        stages["sidecar"] = {"status": "completed", "reason": sidecar_stage}
    except Exception as exc:
        sidecar_summary = {}
        outputs["sidecar_summary_json"] = ""
        stages["sidecar"] = {"status": "skipped_or_failed", "reason": str(exc)}

    evidence_bundle_path = ""
    if sidecar_summary.get("evidence_bundle", {}).get("path"):
        evidence_bundle_path = str(sidecar_summary["evidence_bundle"]["path"])
        outputs["evidence_bundle_json"] = evidence_bundle_path
        stages["evidence_bundle"] = {
            "status": "completed",
            "reason": sidecar_summary.get("evidence_bundle", {}).get("evidence_status", ""),
        }
        if sidecar_summary.get("evidence_bundle_integrity", {}).get("path"):
            outputs["evidence_bundle_integrity_json"] = str(sidecar_summary["evidence_bundle_integrity"]["path"])
            stages["evidence_bundle_integrity"] = {
                "status": "completed",
                "reason": sidecar_summary.get("evidence_bundle_integrity", {}).get("status", ""),
            }
    else:
        outputs["evidence_bundle_json"] = ""
        stages["evidence_bundle"] = {
            "status": "pending",
            "reason": "pressure quick-check and open-flow samples are required",
        }

    report_model_path = ""
    if evidence_bundle_path:
        report_outputs = write_v1_5_calibration_reports(
            evidence_bundle_path=evidence_bundle_path,
            output_dir=destination / "reports",
            reviewer=reviewer,
            approver=approver,
            location=location,
            calibration_date=calibration_date,
            analyzer_prefix=analyzer_prefix,
            uncertainty_json=uncertainty_json,
        )
        report_model_path = str(report_outputs["report_model"])
        outputs.update({f"report_{key}": str(value) for key, value in report_outputs.items()})
        stages["reports"] = {"status": "completed", "reason": "generated from evidence bundle"}
    else:
        stages["reports"] = {"status": "pending", "reason": "evidence bundle missing"}

    samples_status = _check_status(readiness, "open_flow_samples")
    advanced_qc_json = ""
    if samples_status == "pass":
        advanced_outputs = write_advanced_qc_summary(
            run_dir=root,
            output_dir=destination / "advanced_qc",
            analyzer_prefix=analyzer_prefix,
        )
        advanced_qc_json = str(advanced_outputs["summary_json"])
        outputs["advanced_qc_json"] = advanced_qc_json
        outputs["advanced_qc_markdown"] = str(advanced_outputs["summary_markdown"])
        stages["advanced_qc"] = {"status": "completed", "reason": "samples available"}
    else:
        stages["advanced_qc"] = {"status": "pending", "reason": "samples artifact missing"}

    workbench_outputs = write_formal_workbench(
        output_dir=destination / "workbench",
        run_dir=root,
        plan_path=plan_path,
        pressure_reference_path=pressure_reference_path,
        config_path=config_path,
        evidence_bundle_path=evidence_bundle_path or None,
        report_model_path=report_model_path or None,
        uncertainty_json=uncertainty_json,
        sidecar_summary_path=outputs.get("sidecar_summary_json") or None,
        component=component,
        analyzer_prefix=analyzer_prefix,
        reviewer=reviewer,
        approver=approver,
        today=today,
    )
    workbench_json = str(workbench_outputs["model"])
    outputs["workbench_json"] = workbench_json
    outputs["workbench_html"] = str(workbench_outputs["html"])
    stages["workbench"] = {"status": "completed", "reason": "static review surface"}

    operation_outputs = write_operation_console(
        output_dir=destination / "operation_console",
        workbench_model=_load_json(workbench_json),
        role=role,
    )
    operation_json = str(operation_outputs["model"])
    outputs["operation_console_json"] = operation_json
    outputs["operation_console_html"] = str(operation_outputs["html"])
    stages["operation_console"] = {"status": "completed", "reason": "read-only"}

    review_outputs = write_review_surface(
        output_dir=destination / "review_surface",
        formal_workbench=load_json_object(workbench_json),
        operation_console=load_json_object(operation_json),
        advanced_qc=load_json_object(advanced_qc_json),
        role=role,
    )
    outputs["review_surface_json"] = str(review_outputs["model"])
    outputs["review_surface_html"] = str(review_outputs["html"])
    stages["review_surface"] = {"status": "completed", "reason": "unified offline view"}

    readiness_status = str(readiness.get("readiness_status") or "")
    chain_status = "ready_for_reviewer" if readiness_status == "ready_for_reviewer" else "pending_or_blocked"
    summary: Dict[str, Any] = {
        "schema_version": "v1_5_formal_offline_review_chain_v0",
        "generated_at": _now(),
        "run_dir": str(root),
        "output_dir": str(destination),
        "chain_status": chain_status,
        "readiness_status": readiness_status,
        "requires_real_device_authorization": bool(readiness.get("requires_real_device_authorization")),
        "sidecar_only": True,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "controls_valves_or_pace": False,
        "writes_coefficients": False,
        "real_acceptance_evidence": False,
        "stages": stages,
        "outputs": outputs,
    }
    summary_json = destination / "formal_offline_review_chain_summary.json"
    summary_md = destination / "formal_offline_review_chain_summary.md"
    summary_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    summary_md.write_text(render_offline_review_chain_markdown(summary), encoding="utf-8")
    summary["outputs"]["chain_summary_json"] = str(summary_json)
    summary["outputs"]["chain_summary_markdown"] = str(summary_md)
    artifact_manifest_json = destination / "offline_review_chain_artifacts.json"
    summary["outputs"]["chain_artifact_manifest_json"] = str(artifact_manifest_json)
    summary_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    summary_md.write_text(render_offline_review_chain_markdown(summary), encoding="utf-8")
    artifact_manifest = _output_artifact_manifest(summary["outputs"], exclude_keys={"chain_artifact_manifest_json"})
    summary["artifact_manifest"] = {
        "path": str(artifact_manifest_json),
        "status": artifact_manifest.get("status"),
        "artifact_count": artifact_manifest.get("artifact_count"),
        "missing_count": artifact_manifest.get("missing_count"),
    }
    summary_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    summary_md.write_text(render_offline_review_chain_markdown(summary), encoding="utf-8")
    artifact_manifest = _output_artifact_manifest(summary["outputs"], exclude_keys={"chain_artifact_manifest_json"})
    artifact_manifest_json.write_text(
        json.dumps(artifact_manifest, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    return summary
