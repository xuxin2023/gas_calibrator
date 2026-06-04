"""Prepare an offline V1.5 multi-point candidate-fit run directory.

This tool only combines already-recorded V1.5 open-flow evidence. It does not
open COM ports, control routes, control PACE, or write analyzer coefficients.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional


TARGET_KEYS_BY_COMPONENT = {
    "co2": (
        "target_co2_ppm",
        "co2_ppm_target",
        "co2_reference_ppm",
        "co2_certificate_value",
        "certificate_value",
        "target_value",
    ),
    "h2o": (
        "target_h2o_mmol",
        "h2o_mmol_target",
        "h2o_reference_mmol",
        "h2o_certificate_value",
        "certificate_value",
        "target_value",
    ),
}


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Combine V1.5 single-point open-flow runs into an offline no-write multi-point candidate run."
    )
    parser.add_argument("--output-dir", required=True, help="Destination aggregate run directory.")
    parser.add_argument("--component", choices=("co2", "h2o"), default="co2")
    parser.add_argument(
        "--run-dir",
        action="append",
        required=True,
        help="Source fit run directory. Repeat for each point.",
    )
    parser.add_argument(
        "--verification-run-dir",
        action="append",
        default=[],
        help="Source verification run directory. Repeat for each independent verification point.",
    )
    parser.add_argument(
        "--target-override",
        action="append",
        default=[],
        help="Certificate target override in point_tag=value form, e.g. open_flow_100ppm=99.94.",
    )
    parser.add_argument("--run-id", default=None, help="Optional aggregate run id.")
    return parser.parse_args(list(argv) if argv is not None else None)


def _read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: list[Mapping[str, Any]]) -> None:
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(str(key))
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def _parse_overrides(items: Iterable[str]) -> dict[str, float]:
    overrides: dict[str, float] = {}
    for item in items:
        if "=" not in str(item):
            raise ValueError(f"Invalid --target-override, expected point_tag=value: {item}")
        key, value = str(item).split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"Invalid --target-override point tag: {item}")
        overrides[key] = float(value)
    return overrides


def _load_sidecar_metadata(run_dir: Path) -> dict[str, Any]:
    path = run_dir / "formal_open_flow_sidecar_metadata.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _apply_target_override(row: dict[str, Any], *, component: str, override_value: float, source: str) -> None:
    for key in TARGET_KEYS_BY_COMPONENT[component]:
        row[key] = f"{override_value:.12g}"
    row["certificate_target_override_applied"] = "true"
    row["certificate_target_override_value"] = f"{override_value:.12g}"
    row["certificate_target_override_source"] = source


def prepare_multipoint_candidate_run(
    *,
    output_dir: str | Path,
    run_dirs: Iterable[str | Path],
    verification_run_dirs: Iterable[str | Path] = (),
    component: str = "co2",
    target_overrides: Optional[Mapping[str, float]] = None,
    run_id: str | None = None,
) -> dict[str, Path]:
    destination = Path(output_dir).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    overrides = dict(target_overrides or {})
    aggregate_run_id = run_id or destination.name
    rows: list[dict[str, Any]] = []
    source_rows: list[dict[str, Any]] = []

    source_specs: list[tuple[str | Path, str]] = [(path, "fit") for path in run_dirs]
    source_specs.extend((path, "verification") for path in verification_run_dirs)

    for raw_run_dir, sample_role in source_specs:
        run_dir = Path(raw_run_dir).resolve()
        samples_path = run_dir / "samples_machine_readable.csv"
        if not samples_path.exists():
            raise FileNotFoundError(f"samples_machine_readable.csv not found: {run_dir}")
        metadata = _load_sidecar_metadata(run_dir)
        source_row = {
            "source_run_dir": str(run_dir),
            "source_run_id": metadata.get("run_id", run_dir.name),
            "source_samples_path": str(samples_path),
            "source_component": component,
            "source_co2_ppm": metadata.get("co2_source_ppm", ""),
            "source_certificate_co2_ppm": metadata.get("certificate_co2_ppm", ""),
            "source_open_valves": ";".join(str(item) for item in metadata.get("open_valves", [])),
            "sealed_pressure_control": metadata.get("sealed_pressure_control", ""),
            "writes_senco": metadata.get("writes_senco", ""),
            "writes_device_id": metadata.get("writes_device_id", ""),
            "sample_role": sample_role,
        }
        source_rows.append(source_row)
        for index, row in enumerate(_read_csv(samples_path), start=1):
            point_tag = str(row.get("point_tag") or "").strip()
            item = dict(row)
            source_point_id = f"{source_row['source_run_id']}:{point_tag or index}"
            item["aggregate_run_id"] = aggregate_run_id
            item["source_run_id"] = str(source_row["source_run_id"])
            item["source_run_dir"] = str(run_dir)
            item["source_sample_index"] = item.get("sample_index", index)
            item["sample_role"] = sample_role
            item["source_point_identity"] = source_point_id
            if not str(item.get("point_id") or "").strip():
                item["point_id"] = source_point_id
            if not str(item.get("point_key") or "").strip():
                item["point_key"] = source_point_id
            item.setdefault("verification_point_id", "")
            if sample_role == "verification" and not str(item.get("verification_point_id") or "").strip():
                item["verification_point_id"] = source_point_id
            if point_tag in overrides:
                _apply_target_override(
                    item,
                    component=component,
                    override_value=float(overrides[point_tag]),
                    source=f"target_override:{point_tag}",
                )
            else:
                item["certificate_target_override_applied"] = "false"
            rows.append(item)

    if not rows:
        raise ValueError("No sample rows were loaded from source run directories.")

    csv_path = destination / "samples_machine_readable.csv"
    jsonl_path = destination / "samples_machine_readable.jsonl"
    source_runs_path = destination / "source_runs.csv"
    manifest_path = destination / "multipoint_candidate_run_manifest.json"
    _write_csv(csv_path, rows)
    _write_csv(source_runs_path, source_rows)
    with jsonl_path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
    manifest = {
        "schema_version": "v1_5_multipoint_candidate_run_v0",
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "run_id": aggregate_run_id,
        "component": component,
        "row_count": len(rows),
        "source_run_count": len(source_rows),
        "target_overrides": dict(overrides),
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "controls_pace": False,
        "writes_coefficients": False,
        "writes_device_id": False,
        "physical_scope": "offline_aggregate_of_open_flow_component_samples",
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "samples_csv": csv_path,
        "samples_jsonl": jsonl_path,
        "source_runs_csv": source_runs_path,
        "manifest": manifest_path,
    }


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        outputs = prepare_multipoint_candidate_run(
            output_dir=args.output_dir,
            run_dirs=args.run_dir,
            verification_run_dirs=args.verification_run_dir,
            component=args.component,
            target_overrides=_parse_overrides(args.target_override),
            run_id=args.run_id,
        )
    except Exception as exc:
        print(f"Prepare V1.5 multipoint candidate run failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(json.dumps({key: str(value) for key, value in outputs.items()}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
