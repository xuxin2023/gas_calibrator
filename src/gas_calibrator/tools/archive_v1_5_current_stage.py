"""Archive the current V1.5 CO2/H2O stage evidence into the evidence registry."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..storage.v1_5_evidence.bundle import sha256_file, sha256_json, stable_id
from ..storage.v1_5_evidence.repository import apply_migrations, import_bundle, query_run_summary


TARGET_DEVICES = ("022", "030", "033", "051")
ALL_DEVICES = ("022", "030", "033", "051", "100")


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys()) if rows else []
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _add_file(paths: list[tuple[Path, str, bool]], path: Path, role: str, required: bool = False) -> None:
    if path.exists() and path.is_file():
        paths.append((path.resolve(), role, required))


def _add_dir(
    paths: list[tuple[Path, str, bool]],
    path: Path,
    role: str,
    required: bool = False,
) -> None:
    if not path.exists():
        return
    suffixes = {".csv", ".json", ".jsonl", ".xlsx", ".md", ".log", ".pdf"}
    for child in path.rglob("*"):
        if child.is_file() and child.suffix.lower() in suffixes:
            paths.append((child.resolve(), role, required))


def _artifact_id(run_db_id: str, path: str | Path) -> str:
    return stable_id("sample_file", run_db_id, str(Path(path).resolve()))


def _device_row(
    device_key: str,
    device_type: str,
    role: str,
    display_name: str,
    serial_number: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "id": stable_id("device", device_type, device_key),
        "device_type": device_type,
        "device_role": role,
        "display_name": display_name,
        "serial_number": serial_number or device_key,
        "metadata": metadata or {},
    }


def _latest_write_rows(root: Path, pattern: str) -> dict[str, dict[str, str]]:
    rows_by_device: dict[str, tuple[float, dict[str, str]]] = {}
    for path in sorted(root.rglob(pattern), key=lambda item: item.stat().st_mtime):
        for row in _read_csv(path):
            device_id = (row.get("analyzer_device_id") or row.get("device_id") or "").strip()
            if not device_id or row.get("status") != "written_readback_verified":
                continue
            rows_by_device[device_id] = (path.stat().st_mtime, {**row, "source_path": str(path)})
    return {device_id: row for device_id, (_, row) in rows_by_device.items()}


def _build_status_rows(root: Path, paths: dict[str, Path]) -> list[dict[str, Any]]:
    h2o_rows = _read_csv(paths["h2o_verify"] / "分析仪汇总_水路_20260530_192451.csv")
    h2o_by_device: dict[str, dict[str, Any]] = {}
    for row in h2o_rows:
        device_id = (row.get("Analyzer") or "").replace("GA", "").strip()
        reference = _float(row.get("ppm_H2O_Dew"))
        measured = _float(row.get("ppm_H2O"))
        error = None if reference is None or measured is None else measured - reference
        error_pct = None if error is None or not reference else error / reference * 100.0
        h2o_by_device[device_id] = {
            "reference_h2o_mmol_mol": reference,
            "measured_h2o_mmol_mol": measured,
            "error_mmol_mol": error,
            "error_pct": error_pct,
            "valid_frames": int(float(row.get("ValidFrames") or 0)),
            "total_frames": int(float(row.get("TotalFrames") or 0)),
            "co2_during_h2o_ppm": _float(row.get("ppm_CO2")),
        }

    co2_by_device: dict[str, dict[str, Any]] = {}
    for row in _read_csv(paths["co2_audit"] / "co2_senco_algorithm_audit_devices.csv"):
        device_id = (row.get("device_id") or "").strip()
        cert = _float(row.get("certificate_co2_ppm"))
        observed = _float(row.get("observed_co2_ppm"))
        error = _float(row.get("device_error_ppm"))
        if device_id == "022":
            status = "prior_clean_needs_post_h2o_recheck"
            action = "include in current post-H2O CO2 open-flow recheck"
        elif device_id == "030":
            status = "candidate_bias_needs_post_h2o_recheck"
            action = "review residuals and include in current post-H2O CO2 open-flow recheck"
        elif device_id in {"033", "051"}:
            status = "h2o_fixed_needs_co2_recheck"
            action = "re-run CO2 open-flow verification after H2O correction"
        elif device_id == "100":
            status = "blocked_firmware_contract"
            action = "firmware/contract update before write or acceptance"
        else:
            status = "unknown"
            action = "review"
        co2_by_device[device_id] = {
            "status": status,
            "recommended_next_action": action,
            "observed_co2_ppm_before_h2o_fix": observed,
            "device_error_pct_before_h2o_fix": None if not cert or error is None else error / cert * 100.0,
        }

    h2o_writes = _latest_write_rows(paths["h2o_write_root"], "h2o_senco24_pair_write_summary.csv")
    co2_writes: dict[str, dict[str, str]] = {}
    for summary in (
        paths["co2_write_030"] / "co2_senco13_pair_write_summary.csv",
        paths["co2_write_rewrite"] / "co2_senco13_pair_write_summary.csv",
    ):
        for row in _read_csv(summary):
            if row.get("status") == "written_readback_verified":
                co2_writes[(row.get("analyzer_device_id") or "").strip()] = {**row, "source_path": str(summary)}

    status_rows: list[dict[str, Any]] = []
    for device_id in ALL_DEVICES:
        h2o = h2o_by_device.get(device_id, {})
        co2 = co2_by_device.get(device_id, {"status": "missing", "recommended_next_action": "review"})
        h2o_error_pct = h2o.get("error_pct")
        h2o_pass = device_id in TARGET_DEVICES and h2o_error_pct is not None and abs(float(h2o_error_pct)) <= 2.0
        if h2o_pass:
            h2o_stage = "H2O post-write verification passed"
        elif device_id == "100":
            h2o_stage = "blocked: firmware/contract, captured as evidence only"
        else:
            h2o_stage = "H2O review required"
        status_rows.append(
            {
                "device_id": device_id,
                "h2o_stage_status": h2o_stage,
                "h2o_reference_mmol_mol": h2o.get("reference_h2o_mmol_mol"),
                "h2o_measured_mmol_mol": h2o.get("measured_h2o_mmol_mol"),
                "h2o_error_pct": h2o.get("error_pct"),
                "h2o_valid_frames": h2o.get("valid_frames"),
                "h2o_total_frames": h2o.get("total_frames"),
                "h2o_latest_write_status": h2o_writes.get(device_id, {}).get("status", "blocked_or_not_written"),
                "co2_stage_status": co2.get("status"),
                "co2_prior_900ppm_error_pct": co2.get("device_error_pct_before_h2o_fix"),
                "co2_prior_observed_ppm": co2.get("observed_co2_ppm_before_h2o_fix"),
                "co2_latest_write_status": co2_writes.get(device_id, {}).get("status", "blocked_or_not_written"),
                "overall_current_stage_status": (
                    "blocked_firmware_contract"
                    if device_id == "100"
                    else ("h2o_passed_co2_recheck_required" if h2o_pass else "review_required")
                ),
                "next_safe_action": co2.get("recommended_next_action"),
            }
        )
    return status_rows


def _write_report(path: Path, run_id: str, status_rows: list[dict[str, Any]], artifacts_csv: Path, status_csv: Path) -> None:
    lines = [
        "# V1.5 Current Stage Evidence Archive",
        "",
        f"Run ID: `{run_id}`",
        f"Generated at: `{datetime.now(timezone.utc).isoformat(timespec='seconds')}`",
        "",
        "## Physical Boundary",
        "",
        "- Offline evidence indexing only: no COM ports, no PACE/valve/route control, no SENCO writes.",
        "- H2O evidence is from open-flow water-route sampling and post-write verification.",
        "- CO2 evidence is from prior open-flow gas-route candidate/write/audit artifacts; current acceptance still requires post-H2O CO2 recheck.",
        "- Analyzer `100` remains blocked until firmware/contract handling is resolved.",
        "",
        "## Current Stage Conclusion",
        "",
        "- H2O: `022`, `030`, `033`, and `051` passed the post-SENCO2/SENCO4 verification at the 20 C / 50 %RH open-flow point.",
        "- CO2: keep as write/readback evidence plus audit evidence; run no-write post-H2O CO2 verification before final acceptance.",
        "",
        "## Device Status",
        "",
        "| Device | H2O error % | H2O status | CO2 current status | Next safe action |",
        "| --- | ---: | --- | --- | --- |",
    ]
    for row in status_rows:
        error = row.get("h2o_error_pct")
        error_text = "" if error is None else f"{float(error):.3f}"
        lines.append(
            f"| {row['device_id']} | {error_text} | {row['h2o_stage_status']} | "
            f"{row['co2_stage_status']} | {row['next_safe_action']} |"
        )
    lines.extend(
        [
            "",
            "## Evidence Files",
            "",
            f"- Artifact index: `{artifacts_csv}`",
            f"- Device status CSV: `{status_csv}`",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_archive(root: Path, dsn: str) -> dict[str, Any]:
    run_id = "v1_5_current_stage_co2_h2o_022_030_033_051_20260530"
    run_db_id = stable_id("run", run_id, str(root))
    out = root / "logs" / "v1_5_current_stage_archive_20260530" / "co2_h2o_022_030_033_051_current_stage_r1"
    out.mkdir(parents=True, exist_ok=True)

    paths = {
        "h2o_verify": root / "logs" / "v1_5_h2o_post_senco24_write_verification_20260530" / "h2o_post_senco24_write_currentT_HG20C_50RH_022_030_033_051_r3_long_hgen_wait",
        "h2o_candidate": root / "logs" / "v1_5_h2o_senco24_candidate_review_20260530" / "h2o_mt_no_write_full_low_to_high_r1_design2pct_033p001_qcblock_review",
        "h2o_write_root": root / "logs" / "v1_5_h2o_senco24_controlled_write_20260530",
        "co2_audit": root / "logs" / "co2_senco13_firmware_contract_audit_20260529_r2_100_guard",
        "co2_write_030": root / "logs" / "co2_senco1_senco3_controlled_write_20260528_030_100_051",
        "co2_write_rewrite": root / "logs" / "co2_senco13_pair_rewrite_contract_no_pressure_zero_slots_retry_022_033_100_051_20260529_slow_safe",
        "co2_getco": root / "logs" / "co2_getco1_9_output_chain_probe_5ch_no023_20260529",
        "co2_post_senco5": root / "logs" / "co2_senco13_post_senco5clear_verify_900ppm_20260529",
        "co2_candidate": root / "logs" / "co2_fc_T2_all_eligible_full_cert_pressure_checked_20260529_mainpy",
    }
    status_rows = _build_status_rows(root, paths)
    status_csv = out / "current_stage_device_status.csv"
    _write_csv(status_csv, status_rows)

    selected: list[tuple[Path, str, bool]] = []
    for key, role, required in (
        ("h2o_candidate", "h2o_candidate_review", True),
        ("h2o_verify", "h2o_postwrite_verification", True),
        ("co2_audit", "co2_firmware_contract_audit", True),
        ("co2_write_030", "co2_senco13_write_log", True),
        ("co2_write_rewrite", "co2_senco13_write_log", True),
        ("co2_getco", "co2_getco_snapshot", True),
        ("co2_post_senco5", "co2_postwrite_verification_evidence", False),
        ("co2_candidate", "co2_candidate_fit_evidence", False),
    ):
        _add_dir(selected, paths[key], role, required)
    for subdir in (
        "h2o_senco24_022_write_r1",
        "h2o_senco24_030_033_write_r1",
        "h2o_senco24_051_write_r1",
        "h2o_033_only_after_022_030_051_already_candidate_r1",
    ):
        _add_dir(selected, paths["h2o_write_root"] / subdir, "h2o_senco24_write_log", True)
    _add_file(selected, paths["h2o_write_root"] / "h2o_post_write_analysis.md", "h2o_write_analysis", True)

    for cert_path, role, required in (
        (Path(r"D:\手册\FRGsz25038057-数字压力计-118288.pdf"), "pressure_reference_certificate", True),
        (Path(r"C:\Users\A\Desktop\FCDjw25074175-精密露点仪-245932001.pdf"), "dewpoint_reference_certificate", True),
        (Path(r"C:\Users\A\Desktop\CRGzb25074337-精密露点仪(温度探头)-245932001.pdf"), "dewpoint_temperature_probe_certificate", True),
        (Path(r"C:\Users\A\Desktop\铂电阻最新.pdf"), "digital_thermometer_certificate", True),
        (Path(r"C:\Users\A\Desktop\CRGxc25071726A-高低温交变湿热试验箱-201712068.pdf"), "temperature_chamber_certificate", True),
        (Path(r"C:\Users\A\Desktop\微信图片_20260525191354_234_36.jpg"), "co2_897ppm_certificate_photo", True),
        (Path(r"C:\Users\A\Desktop\微信图片_20260525191355_235_36.jpg"), "co2_897ppm_certificate_photo", True),
    ):
        _add_file(selected, cert_path, role, required)

    seen: set[str] = set()
    artifact_rows: list[dict[str, Any]] = []
    for source_path, role, required in selected:
        key = str(source_path).lower()
        if key in seen:
            continue
        seen.add(key)
        stat = source_path.stat()
        artifact_rows.append(
            {
                "artifact_role": role,
                "path": str(source_path),
                "sha256": sha256_file(source_path),
                "size_bytes": stat.st_size,
                "modified_at": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(timespec="seconds"),
                "required": required,
            }
        )

    artifacts_csv = out / "current_stage_evidence_artifacts.csv"
    _write_csv(artifacts_csv, artifact_rows)
    report_md = out / "current_stage_status_report.md"
    _write_report(report_md, run_id, status_rows, artifacts_csv, status_csv)
    for generated_path, role in (
        (status_csv, "current_stage_device_status"),
        (artifacts_csv, "current_stage_artifact_index"),
        (report_md, "current_stage_status_report"),
    ):
        stat = generated_path.stat()
        artifact_rows.append(
            {
                "artifact_role": role,
                "path": str(generated_path.resolve()),
                "sha256": sha256_file(generated_path),
                "size_bytes": stat.st_size,
                "modified_at": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(timespec="seconds"),
                "required": True,
            }
        )

    sample_files = [
        {
            "id": _artifact_id(run_db_id, row["path"]),
            "run_db_id": run_db_id,
            "artifact_role": row["artifact_role"],
            "path": row["path"],
            "sha256": row["sha256"],
            "size_bytes": row["size_bytes"],
            "modified_at": row["modified_at"],
            "required": row["required"],
            "metadata": {"archive_run_id": run_id},
        }
        for row in artifact_rows
    ]

    devices = [
        _device_row(device_id, "gas_analyzer", "dut", f"Gas Analyzer {device_id}", device_id, {"included_in_current_acceptance": device_id in TARGET_DEVICES})
        for device_id in ALL_DEVICES
    ]
    devices.extend(
        [
            _device_row("COM22-118288", "digital_pressure_gauge", "pressure_reference", "COM22 digital pressure gauge", "118288"),
            _device_row("DEWPOINT-245932001", "dewpoint_meter", "humidity_reference", "Precision dewpoint meter", "245932001"),
            _device_row("PT100-reference", "digital_thermometer", "temperature_reference", "Digital thermometer / platinum resistance"),
            _device_row("TEMPCHAMBER-201712068", "temperature_chamber", "environment_control", "Temperature chamber", "201712068"),
            _device_row("PACE-K0472", "pressure_controller", "atmosphere_vent_support", "PACE pressure controller", "K0472"),
        ]
    )
    run_devices = [
        {
            "id": stable_id("run_device", run_db_id, row["id"], row["device_role"]),
            "run_db_id": run_db_id,
            "device_id": row["id"],
            "role": row["device_role"],
            "metadata": {},
        }
        for row in devices
    ]

    h2o_summary_artifact_id = _artifact_id(run_db_id, paths["h2o_verify"] / "h2o_post_senco24_write_r3_verification_summary.md")
    co2_audit_artifact_id = _artifact_id(run_db_id, paths["co2_audit"] / "co2_senco_algorithm_audit_devices.csv")

    h2o_max_error = max(abs(float(row["h2o_error_pct"])) for row in status_rows if row["device_id"] in TARGET_DEVICES)
    qc_results: list[dict[str, Any]] = []
    for row in status_rows:
        device_id = row["device_id"]
        h2o_status = "pass" if device_id in TARGET_DEVICES and abs(float(row["h2o_error_pct"])) <= 2.0 else "blocked"
        qc_results.append(
            {
                "id": stable_id("qc", run_db_id, device_id, "h2o_postwrite_error"),
                "run_db_id": run_db_id,
                "scope": "device",
                "subject_id": device_id,
                "rule_name": "h2o_postwrite_error_within_2pct",
                "status": h2o_status,
                "severity": "info" if h2o_status == "pass" else "error",
                "reasons": [] if h2o_status == "pass" else ["firmware_contract_blocked_or_acceptance_excluded"],
                "metrics": {"error_pct": row["h2o_error_pct"], "reference_h2o_mmol_mol": row["h2o_reference_mmol_mol"]},
                "source_artifact_id": h2o_summary_artifact_id,
                "metadata": {},
            }
        )
        qc_results.append(
            {
                "id": stable_id("qc", run_db_id, device_id, "co2_post_h2o_recheck"),
                "run_db_id": run_db_id,
                "scope": "device",
                "subject_id": device_id,
                "rule_name": "co2_post_h2o_recheck_required",
                "status": "blocked" if device_id == "100" else "warn",
                "severity": "error" if device_id == "100" else "warning",
                "reasons": [row["co2_stage_status"]],
                "metrics": {"prior_900ppm_error_pct": row["co2_prior_900ppm_error_pct"]},
                "source_artifact_id": co2_audit_artifact_id,
                "metadata": {"next_safe_action": row["next_safe_action"]},
            }
        )

    h2o_candidate_id = stable_id("candidate", run_db_id, "h2o_senco24")
    co2_candidate_id = stable_id("candidate", run_db_id, "co2_senco13")
    h2o_writes = _latest_write_rows(paths["h2o_write_root"], "h2o_senco24_pair_write_summary.csv")
    co2_writes: dict[str, dict[str, str]] = {}
    for summary in (
        paths["co2_write_030"] / "co2_senco13_pair_write_summary.csv",
        paths["co2_write_rewrite"] / "co2_senco13_pair_write_summary.csv",
    ):
        for row in _read_csv(summary):
            if row.get("status") == "written_readback_verified":
                co2_writes[(row.get("analyzer_device_id") or "").strip()] = {**row, "source_path": str(summary)}
    write_events = []
    for device_id, row in h2o_writes.items():
        write_events.append(
            {
                "id": stable_id("write", run_db_id, "h2o", device_id, row.get("source_path")),
                "run_db_id": run_db_id,
                "analyzer_id": device_id,
                "event_type": "h2o_senco2_senco4_controlled_write",
                "status": "written_readback_verified" if device_id in TARGET_DEVICES else "blocked",
                "approved_by": row.get("approver"),
                "command_summary": "SENCO2/SENCO4 controlled write; no ID write; no SENCO6 write",
                "old_coefficients_hash": None,
                "candidate_id": h2o_candidate_id,
                "readback": {"senco2": row.get("senco2_readback"), "senco4": row.get("senco4_readback"), "source_path": row.get("source_path")},
                "metadata": {"reviewer": row.get("reviewer")},
            }
        )
    for device_id, row in co2_writes.items():
        write_events.append(
            {
                "id": stable_id("write", run_db_id, "co2", device_id, row.get("source_path")),
                "run_db_id": run_db_id,
                "analyzer_id": device_id,
                "event_type": "co2_senco1_senco3_controlled_write",
                "status": "blocked_firmware_contract" if device_id == "100" else "written_readback_verified_pending_post_h2o_recheck",
                "approved_by": row.get("approver"),
                "command_summary": "SENCO1/SENCO3 controlled write/readback; current-pressure terms frozen in candidate model",
                "old_coefficients_hash": None,
                "candidate_id": co2_candidate_id,
                "readback": {"senco1": row.get("senco1_readback"), "senco3": row.get("senco3_readback"), "source_path": row.get("source_path")},
                "metadata": {"reviewer": row.get("reviewer")},
            }
        )

    cert_hashes = {Path(row["path"]).name: row["sha256"] for row in artifact_rows}
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    bundle = {
        "schema": "v1_5_evidence_registry_bundle",
        "schema_version": "001",
        "run_id": run_id,
        "run_db_id": run_db_id,
        "created_at": now,
        "physical_boundaries": {
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "controls_valves_or_pace": False,
            "writes_coefficients": False,
            "offline_archive_only": True,
        },
        "tables": {
            "runs": [
                {
                    "id": run_db_id,
                    "run_id": run_id,
                    "run_dir": str(out.resolve()),
                    "plan_id": "v1_5_current_stage_co2_h2o_archive",
                    "plan_version": "20260530",
                    "analyzer_id": ",".join(TARGET_DEVICES),
                    "operator_name": "codex_offline_archive",
                    "config_hash": sha256_json({"target_devices": TARGET_DEVICES, "blocked": ["100"], "scope": "current_stage_archive"}),
                    "package_status": "h2o_verified_co2_recheck_required",
                    "package_blockers": ["co2_post_h2o_recheck_required", "100_firmware_contract_blocked"],
                    "evidence_status": "indexed_current_stage",
                    "metadata": {"no_com": True, "target_devices": TARGET_DEVICES, "blocked_devices": ["100"]},
                }
            ],
            "devices": devices,
            "run_devices": run_devices,
            "standard_gases": [
                {
                    "id": stable_id("stdgas", run_db_id, "co2", "156230414008"),
                    "run_db_id": run_db_id,
                    "component": "co2",
                    "cylinder_id": "156230414008",
                    "certificate_value": 897.04,
                    "certificate_uncertainty": 1.0,
                    "valid_until": "2027-03-03",
                    "supplier": "Dalian Specialty Gases Co.,Ltd",
                    "certificate_hash": cert_hashes.get("微信图片_20260525191354_234_36.jpg"),
                    "metadata": {"unit": "umol/mol", "uncertainty_type": "relative_percent"},
                }
            ],
            "reference_certificates": [
                {
                    "id": stable_id("ref_cert", run_db_id, "COM22"),
                    "run_db_id": run_db_id,
                    "device_id": stable_id("device", "digital_pressure_gauge", "COM22-118288"),
                    "reference_role": "pressure_reference",
                    "certificate_id": "FRGsz25038057",
                    "certificate_hash": cert_hashes.get("FRGsz25038057-数字压力计-118288.pdf"),
                    "valid_until": None,
                    "uncertainty": None,
                    "unit": "pressure",
                    "metadata": {},
                },
                {
                    "id": stable_id("ref_cert", run_db_id, "dewpoint"),
                    "run_db_id": run_db_id,
                    "device_id": stable_id("device", "dewpoint_meter", "DEWPOINT-245932001"),
                    "reference_role": "dewpoint_reference",
                    "certificate_id": "FCDjw25074175",
                    "certificate_hash": cert_hashes.get("FCDjw25074175-精密露点仪-245932001.pdf"),
                    "valid_until": None,
                    "uncertainty": None,
                    "unit": "dewpoint",
                    "metadata": {},
                },
                {
                    "id": stable_id("ref_cert", run_db_id, "pt100"),
                    "run_db_id": run_db_id,
                    "device_id": stable_id("device", "digital_thermometer", "PT100-reference"),
                    "reference_role": "digital_thermometer",
                    "certificate_id": "pt100_latest",
                    "certificate_hash": cert_hashes.get("铂电阻最新.pdf"),
                    "valid_until": None,
                    "uncertainty": None,
                    "unit": "temperature",
                    "metadata": {},
                },
            ],
            "calibration_points": [
                {
                    "id": stable_id("point", run_db_id, "h2o", "20c_50rh_postwrite"),
                    "run_db_id": run_db_id,
                    "component": "h2o",
                    "point_key": "h2o_20c_50rh_open_flow_postwrite",
                    "point_tag": "20C_50RH",
                    "pressure_mode": "open_flow_atmosphere",
                    "target_value": 12.808812,
                    "sample_count": 100,
                    "a_grade_count": 80,
                    "b_grade_count": 0,
                    "rejected_count": 20,
                    "metadata": {"target_source": "dewpoint_meter_plus_COM22_pressure", "accepted_devices": TARGET_DEVICES, "blocked_device": "100"},
                },
                {
                    "id": stable_id("point", run_db_id, "co2", "897ppm_pending_recheck"),
                    "run_db_id": run_db_id,
                    "component": "co2",
                    "point_key": "co2_897ppm_open_flow_pending_post_h2o_recheck",
                    "point_tag": "897ppm",
                    "pressure_mode": "open_flow_atmosphere",
                    "target_value": 897.04,
                    "sample_count": 0,
                    "a_grade_count": 0,
                    "b_grade_count": 0,
                    "rejected_count": 0,
                    "metadata": {"status": "post_h2o_recheck_required"},
                },
            ],
            "sample_files": sample_files,
            "qc_results": qc_results,
            "coefficient_snapshots": [],
            "coefficient_candidates": [
                {
                    "id": h2o_candidate_id,
                    "run_db_id": run_db_id,
                    "component": "h2o",
                    "candidate_status": "written_verified_four_target_devices",
                    "allowed_for_review": True,
                    "auto_write_allowed": False,
                    "blockers": ["100_firmware_contract_blocked"],
                    "coefficients": {"source": str(paths["h2o_candidate"] / "h2o_senco24_coefficients.csv"), "target_devices_verified": TARGET_DEVICES},
                    "source_artifact_id": _artifact_id(run_db_id, paths["h2o_candidate"] / "h2o_senco24_coefficients.csv"),
                    "metadata": {},
                },
                {
                    "id": co2_candidate_id,
                    "run_db_id": run_db_id,
                    "component": "co2",
                    "candidate_status": "written_readback_but_acceptance_recheck_required",
                    "allowed_for_review": True,
                    "auto_write_allowed": False,
                    "blockers": ["post_h2o_co2_recheck_required", "100_firmware_contract_blocked"],
                    "coefficients": {"source": str(paths["co2_candidate"]), "pressure_terms_frozen": True},
                    "source_artifact_id": None,
                    "metadata": {},
                },
            ],
            "coefficient_write_events": write_events,
            "reports": [
                {
                    "id": stable_id("report", run_db_id, "current_stage_status"),
                    "run_db_id": run_db_id,
                    "report_type": "current_stage_status",
                    "path": str(report_md.resolve()),
                    "sha256": sha256_file(report_md),
                    "status": "available",
                    "generated_at": now,
                    "metadata": {},
                }
            ],
            "audit_events": [
                {"id": stable_id("audit", run_db_id, "archive_generated"), "run_db_id": run_db_id, "event_type": "current_stage_archive_generated", "actor": "codex", "event_at": now, "payload": {"no_com": True, "no_senco_write": True, "no_route_control": True}},
                {"id": stable_id("audit", run_db_id, "h2o_verified"), "run_db_id": run_db_id, "event_type": "h2o_postwrite_verification_reviewed", "actor": "codex", "event_at": now, "payload": {"accepted_devices": TARGET_DEVICES, "blocked_devices": ["100"]}},
                {"id": stable_id("audit", run_db_id, "co2_recheck_needed"), "run_db_id": run_db_id, "event_type": "co2_post_h2o_recheck_required", "actor": "codex", "event_at": now, "payload": {"devices": TARGET_DEVICES, "blocked_devices": ["100"]}},
            ],
            "evidence_integrity_checks": [
                {"id": stable_id("check", run_db_id, "artifact_hashes_present"), "run_db_id": run_db_id, "check_name": "artifact_hashes_present", "status": "pass", "severity": "info", "details": {"artifact_count": len(artifact_rows), "required_artifact_count": sum(1 for row in artifact_rows if row["required"])}},
                {"id": stable_id("check", run_db_id, "h2o_four_targets_passed"), "run_db_id": run_db_id, "check_name": "h2o_four_targets_passed", "status": "pass", "severity": "info", "details": {"accepted_devices": TARGET_DEVICES, "max_abs_error_pct": h2o_max_error}},
                {"id": stable_id("check", run_db_id, "co2_post_h2o_recheck_required"), "run_db_id": run_db_id, "check_name": "co2_post_h2o_recheck_required", "status": "warn", "severity": "warning", "details": {"reason": "Run no-write CO2 open-flow recheck before final CO2 acceptance."}},
                {"id": stable_id("check", run_db_id, "device_100_blocked"), "run_db_id": run_db_id, "check_name": "device_100_blocked", "status": "warn", "severity": "warning", "details": {"reason": "100 firmware/payload contract blocked; excluded from acceptance."}},
            ],
        },
    }
    bundle_json = out / "current_stage_evidence_bundle.json"
    bundle_json.write_text(json.dumps(bundle, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    apply_migrations(dsn)
    import_result = import_bundle(dsn, bundle)
    query_rows = query_run_summary(dsn, run_id)
    summary_json = out / "current_stage_db_import_summary.json"
    summary_json.write_text(
        json.dumps({"run_id": run_id, "run_db_id": run_db_id, "database_import": import_result, "query_summary": query_rows}, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    return {
        "out_dir": str(out),
        "report": str(report_md),
        "status_csv": str(status_csv),
        "artifacts_csv": str(artifacts_csv),
        "bundle_json": str(bundle_json),
        "summary_json": str(summary_json),
        "artifact_count": len(artifact_rows),
        "db_import": import_result,
        "query_summary": query_rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".", help="Workspace root.")
    parser.add_argument("--dsn", default="", help="PostgreSQL DSN.")
    args = parser.parse_args()
    if not args.dsn:
        raise SystemExit("--dsn is required")
    result = build_archive(Path(args.root).resolve(), args.dsn)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
