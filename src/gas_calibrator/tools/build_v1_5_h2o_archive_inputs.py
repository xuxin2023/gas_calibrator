"""Build offline archive inputs for a completed V1.5 H2O open-flow run.

The tool only aggregates existing evidence files. It does not open COM ports,
control valves, control the humidity generator, or write analyzer coefficients.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence


H2O_POINT_GLOB = "p*_h2o"


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _load_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    header: List[str] = []
    for row in rows:
        for key in row:
            text = str(key)
            if text not in header:
                header.append(text)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in header})


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _first_value(row: Mapping[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _set_blank(row: Dict[str, Any], key: str, value: Any) -> None:
    if row.get(key) in (None, ""):
        row[key] = value


def _copy_file(source: Optional[Path], destination_dir: Path, name: Optional[str] = None) -> Optional[Path]:
    if source is None or not source.exists() or not source.is_file():
        return None
    destination = destination_dir / (name or source.name)
    if source.resolve() != destination.resolve():
        shutil.copy2(source, destination)
    return destination


def _copy_tree_files(source_dir: Optional[Path], destination_dir: Path) -> List[Path]:
    copied: List[Path] = []
    if source_dir is None or not source_dir.exists():
        return copied
    destination_dir.mkdir(parents=True, exist_ok=True)
    for source in sorted(path for path in source_dir.iterdir() if path.is_file()):
        copied_path = destination_dir / source.name
        shutil.copy2(source, copied_path)
        copied.append(copied_path)
    return copied


def _h2o_ppmv_from_mmol(mmol: Optional[float]) -> tuple[Optional[float], Optional[float]]:
    if mmol is None:
        return None, None
    wet_ppmv = mmol * 1000.0
    dry_ppmv = None
    denominator = 1.0 - wet_ppmv / 1_000_000.0
    if denominator > 0:
        dry_ppmv = wet_ppmv / denominator
    return wet_ppmv, dry_ppmv


def _aggregate_samples(run_parent: Path) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    rows: List[Dict[str, Any]] = []
    point_sources: List[Dict[str, Any]] = []
    for point_dir in sorted(run_parent.glob(H2O_POINT_GLOB)):
        sample_path = point_dir / "samples_machine_readable.csv"
        if not sample_path.exists():
            continue
        sample_rows = _load_csv(sample_path)
        point_sources.append(
            {
                "point_dir": str(point_dir),
                "samples_machine_readable_csv": str(sample_path),
                "sample_count": len(sample_rows),
                "sha256": _sha256_file(sample_path),
            }
        )
        for source in sample_rows:
            row: Dict[str, Any] = dict(source)
            target_mmol = _safe_float(
                _first_value(row, ("target_h2o_mmol", "h2o_mmol_target", "h2o_target_mmol", "target_value"))
            )
            wet_ppmv, dry_ppmv = _h2o_ppmv_from_mmol(target_mmol)
            _set_blank(row, "component", "h2o")
            _set_blank(row, "point_phase", "h2o")
            _set_blank(row, "route", "h2o")
            _set_blank(row, "pressure_mode", "ambient_open")
            if target_mmol is not None:
                row["target_h2o_mmol"] = f"{target_mmol:.9g}"
                row["target_value"] = f"{target_mmol:.9g}"
            if wet_ppmv is not None:
                row["h2o_wet_ppmv"] = f"{wet_ppmv:.9g}"
            if dry_ppmv is not None:
                row["h2o_dry_ppmv"] = f"{dry_ppmv:.9g}"
            row["source_point_dir"] = str(point_dir)
            row["source_samples_file"] = str(sample_path)
            rows.append(row)
    return rows, point_sources


def _load_h2o_standard_reference(path: Optional[Path]) -> Dict[str, Any]:
    if path and path.exists():
        data = _load_json(path)
        for row in data.get("standard_gases", []) or []:
            if str(row.get("component") or "").strip().lower() == "h2o":
                return {
                    "component": "h2o",
                    "cylinder_id": row.get("cylinder_id") or row.get("source_id") or "dynamic_humidity_reference",
                    "certificate_value": row.get("certificate_value") or "dynamic_h2o_targets_from_dewpoint_reference",
                    "certificate_uncertainty": row.get("certificate_uncertainty") or "dewpoint_reference_uncertainty",
                    "valid_until": row.get("valid_until") or "2026-08-17",
                    "supplier": row.get("supplier") or "reference_certificates",
                    "certificate_hash": row.get("certificate_hash") or _sha256_file(path),
                    "metadata": row,
                }
    return {
        "component": "h2o",
        "cylinder_id": "dynamic_humidity_reference",
        "certificate_value": "dynamic_h2o_targets_from_dewpoint_reference",
        "certificate_uncertainty": "dewpoint_reference_uncertainty",
        "valid_until": "2026-08-17",
        "supplier": "dewpoint_meter_temperature_pressure_reference_chain",
        "certificate_hash": "dynamic-h2o-reference-chain",
    }


def _queue_reference_points(queue_manifest_path: Optional[Path]) -> List[Dict[str, Any]]:
    if queue_manifest_path is None or not queue_manifest_path.exists():
        return []
    points = []
    for row in _load_csv(queue_manifest_path):
        points.append(
            {
                "point_id": row.get("point_id", ""),
                "temperature_c": row.get("temperature_c", ""),
                "hgen_temperature_c": row.get("hgen_temperature_c", ""),
                "hgen_rh_pct": row.get("hgen_rh_pct", ""),
                "reference_dewpoint_c": row.get("reference_dewpoint_c", ""),
                "reference_h2o_mmol": row.get("reference_h2o_mmol", ""),
                "min_purge_s": row.get("min_purge_s", ""),
                "sample_count": row.get("sample_count", ""),
            }
        )
    return points


def _device_ids_from_rows(rows: Sequence[Mapping[str, Any]]) -> List[str]:
    device_ids = set()
    for row in rows:
        for key, value in row.items():
            text = str(key or "")
            if text.startswith("ga") and text.endswith("_mode2_tokens_json") and value:
                try:
                    tokens = json.loads(str(value))
                except Exception:
                    tokens = []
                if isinstance(tokens, list) and len(tokens) >= 2 and str(tokens[1]).strip():
                    device_ids.add(str(tokens[1]).strip())
    return sorted(device_ids)


def _adapt_pressure_rows(source_csv: Path, device_ids: Sequence[str]) -> List[Dict[str, Any]]:
    rows = []
    device_set = {str(item).zfill(3) for item in device_ids}
    for source in _load_csv(source_csv):
        device_id = str(source.get("analyzer_device_id") or "").strip().zfill(3)
        if device_id not in device_set:
            continue
        row: Dict[str, Any] = dict(source)
        row["source_analyzer_prefix"] = row.get("analyzer_prefix", "")
        row["analyzer_prefix"] = f"ga{device_id}"
        row["analyzer_device_id"] = device_id
        rows.append(row)
    return rows


def build_archive_inputs(
    *,
    run_parent: Path,
    queue_run_dir: Path,
    output_dir: Path,
    candidate_review_dir: Optional[Path],
    old_component_snapshot_dir: Optional[Path],
    pressure_check_csv: Path,
    pressure_reference_json: Path,
    humidity_reference_json: Optional[Path],
    standard_reference_json: Optional[Path],
    operator: str,
    calibration_date: str,
) -> Dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    aggregate_rows, point_sources = _aggregate_samples(run_parent)
    if not aggregate_rows:
        raise FileNotFoundError(f"No H2O samples_machine_readable.csv files found under {run_parent}")

    device_ids = _device_ids_from_rows(aggregate_rows)
    samples_csv = output_dir / "samples_machine_readable.csv"
    _write_csv(samples_csv, aggregate_rows)

    copied_files: List[Path] = []
    for source_name in (
        "queue_summary.json",
        "queue_manifest.csv",
        "h2o_mt_no_write_20260530_full_low_to_high_r1_process_record.json",
    ):
        copied = _copy_file(queue_run_dir / source_name, output_dir)
        if copied:
            copied_files.append(copied)
    for source in sorted(queue_run_dir.parent.glob("*process_record.json")):
        copied = _copy_file(source, output_dir)
        if copied:
            copied_files.append(copied)
    for source in sorted(queue_run_dir.parent.glob("*process_stdout.log")):
        copied = _copy_file(source, output_dir)
        if copied:
            copied_files.append(copied)
    candidate_destination = output_dir / "h2o_senco24_candidate_review"
    copied_files.extend(_copy_tree_files(candidate_review_dir, candidate_destination))
    old_snapshot_destination = output_dir / "old_getco_component_snapshot"
    copied_files.extend(_copy_tree_files(old_component_snapshot_dir, old_snapshot_destination))
    pressure_reference_copy = _copy_file(pressure_reference_json, output_dir, "com22_pressure_reference.json")
    if pressure_reference_copy:
        copied_files.append(pressure_reference_copy)
    humidity_reference_copy = _copy_file(
        humidity_reference_json,
        output_dir,
        "humidity_temperature_reference_certificates_20260525.json",
    )
    if humidity_reference_copy:
        copied_files.append(humidity_reference_copy)
    standard_reference_copy = _copy_file(
        standard_reference_json,
        output_dir,
        "standard_h2o_reference_snapshot.json",
    )
    if standard_reference_copy:
        copied_files.append(standard_reference_copy)

    adapted_pressure_rows = _adapt_pressure_rows(pressure_check_csv, device_ids)
    if not adapted_pressure_rows:
        raise ValueError(
            f"Pressure quick-check {pressure_check_csv} did not contain the H2O device ids {device_ids}"
        )
    pressure_check_copy = output_dir / "pressure_channel_quick_check_h2o_archive.csv"
    _write_csv(pressure_check_copy, adapted_pressure_rows)

    queue_manifest_copy = output_dir / "queue_manifest.csv"
    queue_summary_copy = output_dir / "queue_summary.json"
    queue_summary = _load_json(queue_summary_copy) if queue_summary_copy.exists() else {}
    config_hash_source = queue_summary_copy if queue_summary_copy.exists() else samples_csv
    h2o_standard = _load_h2o_standard_reference(standard_reference_copy)
    plan = {
        "plan_id": "v1_5_formal_h2o_full_temp_no_write_archive_20260530",
        "plan_version": "2026-05-30",
        "config_hash": _sha256_file(config_hash_source),
        "operator": operator,
        "analyzer_id": "multi_analyzer_h2o_archive",
        "allow_device_write": False,
        "standard_gases": [h2o_standard],
        "calibration_scope": "V1.5 open-flow H2O full-temperature no-write archive",
        "physical_boundaries": {
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "controls_valves_or_pace": False,
            "writes_coefficients": False,
            "no_write": True,
        },
        "h2o_reference_points": _queue_reference_points(queue_manifest_copy),
        "source_queue_summary": queue_summary,
    }
    plan_path = output_dir / "formal_plan_h2o_snapshot.json"
    _write_json(plan_path, plan)

    manifest = {
        "schema": "v1_5_h2o_archive_inputs_manifest_v0",
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "run_parent": str(run_parent),
        "queue_run_dir": str(queue_run_dir),
        "output_dir": str(output_dir),
        "calibration_date": calibration_date,
        "device_ids": device_ids,
        "sample_count": len(aggregate_rows),
        "point_sources": point_sources,
        "pressure_quick_check_source": str(pressure_check_csv),
        "pressure_quick_check_archive": str(pressure_check_copy),
        "pressure_row_count": len(adapted_pressure_rows),
        "candidate_review_dir": str(candidate_review_dir) if candidate_review_dir else "",
        "old_component_snapshot_dir": str(old_component_snapshot_dir) if old_component_snapshot_dir else "",
        "copied_files": [str(path) for path in copied_files],
        "physical_meaning": (
            "H2O archive rows combine wet open-flow samples from the completed V1.5 water route. "
            "Dewpoint-derived H2O targets and analyzer H2O ratio/signal fields are preserved per device id. "
            "Pressure quick-check rows are remapped from acquisition-channel prefixes to analyzer device-id "
            "prefixes for identity-safe traceability only."
        ),
        "no_hardware_side_effects": {
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
        },
    }
    manifest_path = output_dir / "h2o_archive_input_manifest.json"
    _write_json(manifest_path, manifest)

    return {
        "samples_csv": samples_csv,
        "plan_json": plan_path,
        "pressure_reference_json": pressure_reference_copy or pressure_reference_json,
        "pressure_check_csv": pressure_check_copy,
        "manifest_json": manifest_path,
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-parent", required=True)
    parser.add_argument("--queue-run-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--candidate-review-dir")
    parser.add_argument("--old-component-snapshot-dir")
    parser.add_argument("--pressure-check-csv", required=True)
    parser.add_argument("--pressure-reference-json", required=True)
    parser.add_argument("--humidity-reference-json")
    parser.add_argument("--standard-reference-json")
    parser.add_argument("--operator", default="offline_archive_closure")
    parser.add_argument("--calibration-date", default="2026-05-30")
    args = parser.parse_args(argv)

    outputs = build_archive_inputs(
        run_parent=Path(args.run_parent).resolve(),
        queue_run_dir=Path(args.queue_run_dir).resolve(),
        output_dir=Path(args.output_dir).resolve(),
        candidate_review_dir=Path(args.candidate_review_dir).resolve() if args.candidate_review_dir else None,
        old_component_snapshot_dir=(
            Path(args.old_component_snapshot_dir).resolve() if args.old_component_snapshot_dir else None
        ),
        pressure_check_csv=Path(args.pressure_check_csv).resolve(),
        pressure_reference_json=Path(args.pressure_reference_json).resolve(),
        humidity_reference_json=Path(args.humidity_reference_json).resolve() if args.humidity_reference_json else None,
        standard_reference_json=Path(args.standard_reference_json).resolve() if args.standard_reference_json else None,
        operator=args.operator,
        calibration_date=args.calibration_date,
    )
    print(json.dumps({key: str(value) for key, value in outputs.items()}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
