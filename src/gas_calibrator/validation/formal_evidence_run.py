"""Prepare and orchestrate V1.5 formal evidence sidecars.

These helpers only read and write evidence files. They do not open COM ports,
control valves/routes/PACE, or write analyzer coefficients.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence

from .formal_calibration_package import write_formal_calibration_package
from .formal_contracts import COM22_PRESSURE_REFERENCE_TEMPLATE, FORMAL_PLAN_TEMPLATE
from .formal_preflight import write_formal_preflight_report
from ..storage.v1_5_evidence.bundle import (
    build_evidence_bundle,
    bundle_summary,
    verify_evidence_bundle_integrity,
    write_bundle_json,
)
from ..storage.v1_5_evidence.repository import apply_migrations, import_bundle


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: str | Path | None, default: Mapping[str, Any]) -> Dict[str, Any]:
    if path is None:
        return dict(default)
    source = Path(path)
    if not source.exists():
        raise FileNotFoundError(f"JSON file not found: {source}")
    return json.loads(source.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return path


def _config_hash(config_path: str | Path | None) -> str:
    if not config_path:
        return "<sha256-runtime-config>"
    path = Path(config_path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"Runtime config not found: {path}")
    return sha256_file(path)


def _default_run_id() -> str:
    return f"v1_5_formal_evidence_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def prepare_formal_evidence_run(
    *,
    output_dir: str | Path,
    operator: str,
    analyzer_id: str,
    run_id: str | None = None,
    plan_id: str | None = None,
    plan_version: str | None = None,
    config_path: str | Path | None = None,
    standard_gases_json: str | Path | None = None,
    pressure_reference_json: str | Path | None = None,
    lab: str = "",
    ambient_temperature_c: Any = None,
    ambient_rh_pct: Any = None,
) -> Dict[str, Path]:
    """Create plan/reference/manifest files for a formal V1.5 evidence run."""

    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    resolved_run_id = str(run_id or _default_run_id())
    version = str(plan_version or datetime.now().date().isoformat())

    plan = dict(FORMAL_PLAN_TEMPLATE)
    if standard_gases_json:
        gases_payload = _load_json(standard_gases_json, {})
        gases: Any = gases_payload.get("standard_gases") if isinstance(gases_payload, Mapping) else gases_payload
        if not isinstance(gases, Sequence) or isinstance(gases, (str, bytes, bytearray)):
            raise ValueError("standard gases JSON must contain a list or a 'standard_gases' list")
        plan["standard_gases"] = [dict(item) for item in gases if isinstance(item, Mapping)]
    plan.update(
        {
            "plan_id": str(plan_id or resolved_run_id),
            "plan_version": version,
            "config_hash": _config_hash(config_path),
            "operator": str(operator),
            "analyzer_id": str(analyzer_id),
            "allow_candidate_coefficients": True,
            "allow_device_write": False,
            "formal_run_id": resolved_run_id,
            "formal_execution_order": [
                "PRECHECK",
                "PRESSURE_CHANNEL_QUICK_CHECK",
                "OPEN_FLOW_PURGE",
                "STABILITY_GATE",
                "SAMPLE_WINDOW",
                "QC_AND_REPORT",
                "CANDIDATE_REVIEW",
            ],
            "formal_boundaries": {
                "route_mode": "open_flow_component_calibration",
                "sealed_pressure_points_enter_formal_fit": False,
                "pace_output_long_open_flow_dynamic_control": "diagnostic_only",
                "vent_hold_pressure_points": "diagnostic_only",
                "device_write_default": False,
            },
        }
    )
    environment = dict(plan.get("environment") or {})
    if lab:
        environment["lab"] = lab
    if ambient_temperature_c not in (None, ""):
        environment["ambient_temperature_c"] = ambient_temperature_c
    if ambient_rh_pct not in (None, ""):
        environment["ambient_rh_pct"] = ambient_rh_pct
    plan["environment"] = environment
    if config_path:
        plan["runtime_config_path"] = str(Path(config_path).resolve())

    pressure_reference = _load_json(pressure_reference_json, COM22_PRESSURE_REFERENCE_TEMPLATE)
    pressure_reference.setdefault("unit", "hPa")
    pressure_reference.setdefault("reference_role", "primary_pressure_reference")

    plan_path = _write_json(root / "formal_plan_snapshot.json", plan)
    pressure_reference_path = _write_json(root / "com22_pressure_reference.json", pressure_reference)

    manifest = {
        "manifest_type": "v1_5_formal_evidence_run_manifest",
        "schema_version": "001",
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "run_id": resolved_run_id,
        "sidecar_only": True,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "controls_valves_or_pace": False,
        "writes_coefficients": False,
        "formal_plan_snapshot": {
            "path": str(plan_path),
            "sha256": sha256_file(plan_path),
        },
        "pressure_reference_snapshot": {
            "path": str(pressure_reference_path),
            "sha256": sha256_file(pressure_reference_path),
        },
        "runtime_config": {
            "path": str(Path(config_path).resolve()) if config_path else "",
            "sha256": plan.get("config_hash", ""),
        },
        "required_future_artifacts": [
            "pressure_channel_quick_check_<run_id>.csv",
            "samples_<timestamp>.csv",
            "formal_preflight_report",
            "formal_calibration_package",
            "evidence_bundle.json",
        ],
    }
    manifest_path = _write_json(root / "evidence_run_manifest.json", manifest)
    return {
        "plan": plan_path,
        "pressure_reference": pressure_reference_path,
        "manifest": manifest_path,
    }


def run_formal_evidence_sidecar(
    *,
    run_dir: str | Path,
    plan_path: str | Path,
    pressure_reference_path: str | Path,
    config_path: str | Path | None = None,
    output_dir: str | Path | None = None,
    stage: str = "all",
    component: str = "both",
    analyzer_prefix: str = "ga01",
    require_quick_check_artifact: bool = True,
    today: Any = None,
    dsn: str | None = None,
    apply_db_migrations: bool = False,
    import_db: bool = False,
) -> Dict[str, Any]:
    """Run preflight/package/bundle/optional DB import for existing artifacts."""

    root = Path(run_dir).resolve()
    destination = Path(output_dir).resolve() if output_dir else root / "formal_evidence_sidecar"
    destination.mkdir(parents=True, exist_ok=True)
    normalized_stage = str(stage or "all").strip().lower()
    if normalized_stage not in {"preflight", "package", "all"}:
        raise ValueError("stage must be one of: preflight, package, all")

    summary: Dict[str, Any] = {
        "run_dir": str(root),
        "output_dir": str(destination),
        "stage": normalized_stage,
        "sidecar_only": True,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "controls_valves_or_pace": False,
        "writes_coefficients": False,
        "preflight": {},
        "formal_package": {},
        "evidence_bundle": {},
        "evidence_bundle_integrity": {},
        "database_imported": False,
    }

    if normalized_stage in {"preflight", "all"}:
        preflight_outputs = write_formal_preflight_report(
            run_dir=root,
            plan_path=plan_path,
            pressure_reference_path=pressure_reference_path,
            config_path=config_path,
            output_dir=destination / "formal_preflight_report",
            component=component,
            analyzer_prefix=analyzer_prefix,
            require_quick_check_artifact=require_quick_check_artifact,
            today=today,
        )
        summary["preflight"] = {key: str(value) for key, value in preflight_outputs.items()}

    if normalized_stage == "preflight":
        _write_json(destination / "formal_evidence_sidecar_summary.json", summary)
        return summary

    package_outputs = write_formal_calibration_package(
        run_dir=root,
        output_dir=destination / "formal_calibration_package",
        plan_path=plan_path,
        pressure_reference_path=pressure_reference_path,
        component=component,
        analyzer_prefix=analyzer_prefix,
        require_quick_check_artifact=require_quick_check_artifact,
        today=today,
    )
    summary["formal_package"] = {key: str(value) for key, value in package_outputs.items()}

    bundle = build_evidence_bundle(
        run_dir=root,
        plan_path=plan_path,
        pressure_reference_path=pressure_reference_path,
        component=component,
        analyzer_prefix=analyzer_prefix,
        require_quick_check_artifact=require_quick_check_artifact,
        today=today,
    )
    bundle_path = write_bundle_json(bundle, destination / "evidence_bundle.json")
    summary["evidence_bundle"] = bundle_summary(bundle)
    summary["evidence_bundle"]["path"] = str(bundle_path)
    integrity = verify_evidence_bundle_integrity(bundle)
    integrity_path = _write_json(destination / "evidence_bundle_integrity.json", integrity)
    summary["evidence_bundle_integrity"] = {
        "path": str(integrity_path),
        "status": integrity.get("status"),
        "failed_check_count": integrity.get("failed_check_count"),
        "check_count": integrity.get("check_count"),
    }

    if import_db:
        if not dsn:
            raise ValueError("Database import requested but no DSN was provided")
        if apply_db_migrations:
            summary["migrations_applied"] = apply_migrations(dsn)
        summary["database_import"] = import_bundle(dsn, bundle)
        summary["database_imported"] = True

    _write_json(destination / "formal_evidence_sidecar_summary.json", summary)
    return summary
