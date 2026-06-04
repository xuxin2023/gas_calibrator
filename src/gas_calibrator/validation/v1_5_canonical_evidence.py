"""Canonical simulated V1.5 formal evidence package.

The helpers here create a deterministic offline package for regression,
database import, reporting, and UI review. They do not open COM ports, control
water/gas routes, command PACE or valves, or write analyzer coefficients.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

from .formal_evidence_run import prepare_formal_evidence_run, run_formal_evidence_sidecar, sha256_file
from .formal_reports import write_v1_5_calibration_reports
from .pressure_channel import write_pressure_quick_check_csv


CANONICAL_RUN_ID = "v1_5_canonical_900ppm_open_flow"
CANONICAL_DATE = "2026-05-24"


def _write_json(path: Path, payload: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return path


def _safe_config() -> Dict[str, Any]:
    return {
        "workflow": {
            "controlled_write": False,
            "postrun_corrected_delivery": {"enabled": False, "write_devices": False},
        },
        "validation": {
            "dry_collect": {"write_coefficients": False},
            "coefficient_roundtrip": {"write_back_same": False, "allow_write_modified": False},
        },
        "devices": {
            "pressure_controller": {"present": True, "role": "auxiliary_pressure_reference"},
            "pressure_gauge": {"present": True, "role": "primary_pressure_reference", "id": "COM22-DPG-001"},
        },
        "sencos": {},
    }


def _standard_gases() -> Dict[str, Any]:
    return {
        "standard_gases": [
            {
                "component": "co2",
                "cylinder_id": "CO2-900-CANONICAL",
                "certificate_id": "CO2-CERT-CANONICAL-001",
                "certificate_value": 900.0,
                "certificate_unit": "ppm",
                "certificate_uncertainty": 0.9,
                "uncertainty_coverage_factor": 2.0,
                "valid_until": "2027-01-01",
                "supplier": "simulated-standard-lab",
                "certificate_hash": "canonical-co2-certificate-hash",
            },
            {
                "component": "h2o",
                "cylinder_id": "H2O-GEN-CANONICAL",
                "certificate_id": "H2O-CERT-CANONICAL-001",
                "certificate_value": 0.5,
                "certificate_unit": "mmol/mol",
                "certificate_uncertainty": 0.01,
                "uncertainty_coverage_factor": 2.0,
                "valid_until": "2027-01-01",
                "supplier": "simulated-humidity-lab",
                "certificate_hash": "canonical-h2o-reference-hash",
            },
        ],
        "sidecar_only": True,
        "allow_device_write": False,
    }


def _pressure_reference() -> Dict[str, Any]:
    return {
        "device_id": "COM22-DPG-001",
        "certificate_id": "P-CERT-CANONICAL-001",
        "certificate_uncertainty": 0.15,
        "valid_until": "2027-01-01",
        "certificate_hash": "canonical-com22-pressure-certificate-hash",
        "supplier": "simulated-pressure-lab",
        "unit": "hPa",
    }


def _mode2_tokens(co2_ppm: float, h2o_mmol: float) -> str:
    return json.dumps(
        [
            "YGAS",
            "001",
            f"{co2_ppm:.3f}",
            f"{h2o_mmol:.3f}",
            "1768.000",
            "00.410",
        ],
        separators=(",", ":"),
    )


def _sample_row(index: int, component: str, *, pressure_mode: str = "ambient_open") -> Dict[str, Any]:
    pressure_hpa = 1000.5 + index * 0.002
    co2_ppm = 900.0 + index * 0.01
    h2o_mmol = 0.5 + index * 0.0001
    return {
        "sample_index": index,
        "sample_ts": f"{CANONICAL_DATE}T12:00:{index:02d}",
        "point_phase": component,
        "route": component,
        "pressure_mode": pressure_mode,
        "pressure_gauge_hpa": pressure_hpa,
        "controller_pressure": pressure_hpa + 0.1,
        "pressure_atmosphere_hold_status": "verified",
        "pressure_atmosphere_hold_active": "true",
        "pressure_atmosphere_hold_strategy": "legacy_hold_thread",
        "dewpoint_c": -30.0 + index * 0.001,
        "h2o_dry_ppmv": 500.0 + index * 0.02,
        "h2o_wet_ppmv": 505.0 + index * 0.02,
        "ga01_frame_usable": "true",
        "ga01_mode2_contract_status": "pass",
        "ga01_mode2_qc_status": "pass",
        "ga01_mode2_tokens_json": _mode2_tokens(co2_ppm, h2o_mmol),
        "ga01_raw": "YGAS,001,...",
        "ga01_ref_signal": 3322.0 + index * 0.01,
        "ga01_co2_signal": 4356.0 + index * 0.01,
        "ga01_h2o_signal": 2631.0 + index * 0.005,
        "ga01_chamber_temp_c": 25.0 + index * 0.001,
        "ga01_case_temp_c": 25.5 + index * 0.001,
        "ga01_pressure_kpa": pressure_hpa / 10.0,
        "ga01_co2_ratio_f": 1.3000 + index * 0.0001,
        "ga01_co2_ppm": co2_ppm,
        "ga01_h2o_ratio_f": 0.7000 + index * 0.00001,
        "ga01_h2o_mmol": h2o_mmol,
    }


def canonical_sample_rows(*, include_sealed_diagnostic: bool = True) -> List[Dict[str, Any]]:
    rows = [_sample_row(i, "co2") for i in range(1, 11)]
    rows.extend(_sample_row(i, "h2o") for i in range(11, 21))
    if include_sealed_diagnostic:
        diagnostic = _sample_row(21, "co2", pressure_mode="sealed_controlled")
        diagnostic.update(
            {
                "point_tag": "sealed_pressure_diagnostic",
                "sample_role": "engineering_diagnostic_only",
                "not_real_acceptance_evidence": "true",
            }
        )
        rows.append(diagnostic)
    return rows


def _write_csv(path: Path, rows: Iterable[Mapping[str, Any]]) -> Path:
    materialized = [dict(row) for row in rows]
    keys: List[str] = []
    for row in materialized:
        for key in row:
            if key not in keys:
                keys.append(str(key))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(materialized)
    return path


def _artifact_role(path: Path, root: Path) -> str:
    relative = str(path.resolve().relative_to(root.resolve())).replace("\\", "/").lower()
    name = path.name.lower()
    if name == "samples_20260524.csv":
        return "raw_samples"
    if "pressure_channel_quick_check" in name:
        return "pressure_channel_quick_check"
    if name == "formal_plan_snapshot.json":
        return "formal_plan_snapshot"
    if name == "com22_pressure_reference.json":
        return "pressure_reference_snapshot"
    if "evidence_bundle.json" in name:
        return "evidence_bundle"
    if "formal_calibration_report" in relative:
        return "formal_report"
    if "technical_report" in relative:
        return "technical_report"
    if "run_report" in relative:
        return "run_report"
    if "formal_workbench" in relative:
        return "formal_workbench"
    if "formal_preflight" in relative:
        return "formal_preflight"
    if "formal_calibration_package" in relative:
        return "formal_calibration_package"
    return "supporting_evidence"


def _manifest(root: Path, *, run_id: str, sidecar_summary: Mapping[str, Any], reports: Mapping[str, Path]) -> Dict[str, Any]:
    artifacts: List[Dict[str, Any]] = []
    for path in sorted(root.rglob("*"), key=lambda item: str(item)):
        if not path.is_file() or path.name == "canonical_manifest.json":
            continue
        artifacts.append(
            {
                "relative_path": str(path.resolve().relative_to(root.resolve())).replace("\\", "/"),
                "artifact_role": _artifact_role(path, root),
                "sha256": sha256_file(path),
                "size_bytes": path.stat().st_size,
            }
        )
    return {
        "manifest_type": "v1_5_canonical_simulated_formal_evidence_package",
        "schema_version": "001",
        "run_id": run_id,
        "created_for": "offline_regression_and_review",
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "sidecar_only": True,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "controls_valves_or_pace": False,
        "writes_coefficients": False,
        "physical_meaning": {
            "standard_gas": "900 ppm CO2 simulated certificate value with H2O reference.",
            "pressure_reference": "COM22 simulated traceable pressure reference at current atmosphere.",
            "pressure_channel": "Analyzer internal pressure P is compared with COM22 while continuous atmosphere hold is verified.",
            "open_flow": "CO2/H2O samples represent continuously refreshed open-flow standard gas.",
            "water_route": "H2O open-flow rows preserve dewpoint, dry/wet water-vapor ppmv, H2O ratio, H2O signal, and H2O mmol/mol evidence.",
            "diagnostic_boundary": "Sealed diagnostic rows are retained but excluded from formal fit eligibility.",
        },
        "sidecar_summary": dict(sidecar_summary),
        "report_outputs": {key: str(value.resolve()) for key, value in reports.items()},
        "artifact_count": len(artifacts),
        "artifacts": artifacts,
    }


def write_canonical_v1_5_evidence_package(
    output_dir: str | Path,
    *,
    run_id: str = CANONICAL_RUN_ID,
    today: str = CANONICAL_DATE,
    include_reports: bool = True,
) -> Dict[str, Path]:
    """Create the canonical simulated V1.5 no-write evidence package."""

    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    inputs_dir = root / "inputs"
    prepared_dir = root / "prepared"
    run_dir = root / "run"
    sidecar_dir = run_dir / "formal_evidence_sidecar"

    config_path = _write_json(inputs_dir / "runtime_config_snapshot.json", _safe_config())
    standard_gases_path = _write_json(inputs_dir / "standard_gases.json", _standard_gases())
    pressure_reference_source_path = _write_json(inputs_dir / "com22_pressure_reference_source.json", _pressure_reference())
    prepared = prepare_formal_evidence_run(
        output_dir=prepared_dir,
        operator="canonical-operator",
        analyzer_id="GA-CANONICAL-001",
        run_id=run_id,
        plan_id=f"{run_id}_plan",
        plan_version=today,
        config_path=config_path,
        standard_gases_json=standard_gases_path,
        pressure_reference_json=pressure_reference_source_path,
        lab="canonical-simulated-lab",
        ambient_temperature_c="24.5",
        ambient_rh_pct="45",
    )

    rows = canonical_sample_rows(include_sealed_diagnostic=True)
    samples_path = _write_csv(run_dir / "samples_20260524.csv", rows)
    quick_check_path = write_pressure_quick_check_csv(run_dir, rows[:10], run_id="20260524")
    sidecar_summary = run_formal_evidence_sidecar(
        run_dir=run_dir,
        plan_path=prepared["plan"],
        pressure_reference_path=prepared["pressure_reference"],
        config_path=config_path,
        output_dir=sidecar_dir,
        today=today,
    )

    reports: Dict[str, Path] = {}
    if include_reports:
        evidence_bundle_path = Path(sidecar_summary["evidence_bundle"]["path"])
        reports = write_v1_5_calibration_reports(
            evidence_bundle_path=evidence_bundle_path,
            output_dir=root / "reports",
            report_no="RPT-CANONICAL-001",
            reviewer="canonical-reviewer",
            approver="",
            location="canonical-simulated-lab",
            calibration_date=today,
        )

    manifest_path = _write_json(root / "canonical_manifest.json", _manifest(root, run_id=run_id, sidecar_summary=sidecar_summary, reports=reports))
    readme_path = root / "README.md"
    readme_path.write_text(
        "\n".join(
            [
                "# V1.5 Canonical Simulated Evidence Package",
                "",
                "This package is simulated and sidecar-only.",
                "It does not open COM ports, control water/gas routes, control valves/PACE, or write coefficients.",
                "Use it for regression, database import, report regeneration, and UI review only.",
                "",
                "Physical scope:",
                "- 900 ppm CO2 open-flow component calibration evidence.",
                "- H2O open-flow evidence with dewpoint, dry/wet water-vapor ppmv, ratio, signal, and mmol/mol fields.",
                "- COM22 current-atmosphere pressure-channel quick-check evidence.",
                "- MODE2/factory signal fields preserved in raw samples.",
                "- Sealed diagnostic row retained but excluded from formal fit eligibility.",
            ]
        ),
        encoding="utf-8",
    )

    return {
        "root": root,
        "config": config_path,
        "standard_gases": standard_gases_path,
        "pressure_reference_source": pressure_reference_source_path,
        "plan": prepared["plan"],
        "pressure_reference": prepared["pressure_reference"],
        "evidence_run_manifest": prepared["manifest"],
        "samples": samples_path,
        "pressure_quick_check": quick_check_path,
        "sidecar_summary": sidecar_dir / "formal_evidence_sidecar_summary.json",
        "evidence_bundle": Path(sidecar_summary["evidence_bundle"]["path"]),
        "canonical_manifest": manifest_path,
        "readme": readme_path,
        **{f"report_{key}": value for key, value in reports.items()},
    }
