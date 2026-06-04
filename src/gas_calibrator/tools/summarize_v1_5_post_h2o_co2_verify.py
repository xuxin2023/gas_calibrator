"""Summarize V1.5 post-H2O CO2 no-write verification evidence.

The tool is offline-only: it reads point artifacts and writes CSV/JSON/Markdown
summaries. It never opens COM ports, changes valves, or writes analyzer
coefficients.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional


DEFAULT_TARGET_DEVICES = ("022", "030", "033", "051")
DEFAULT_EXCLUDED_DEVICES: tuple[str, ...] = ()


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, rows: list[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fields:
                fields.append(str(key))
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _first_existing(path: Path, patterns: Iterable[str]) -> Optional[Path]:
    for pattern in patterns:
        matches = sorted(path.glob(pattern), key=lambda item: item.stat().st_mtime, reverse=True)
        if matches:
            return matches[0]
    return None


def _target_from_command(row: Mapping[str, Any]) -> Optional[float]:
    command = str(row.get("command") or "")
    token = "--certificate-co2-ppm "
    if token not in command:
        return None
    tail = command.split(token, 1)[1].strip()
    if not tail:
        return None
    return _safe_float(tail.split()[0])


def _point_dir(output_dir: Path, point_run_id: str) -> Path:
    return output_dir / point_run_id


def _normalize_device_id(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _normalize_analyzer_label(value: Any) -> str:
    return str(value or "").strip().lower()


def _analyzer_device_map(point_path: Path) -> dict[str, str]:
    snapshot = _read_json(point_path / "runtime_config_snapshot.json")
    devices = snapshot.get("devices") if isinstance(snapshot, Mapping) else {}
    if not isinstance(devices, Mapping):
        return {}

    mapping: dict[str, str] = {}
    analyzers = devices.get("gas_analyzers")
    if isinstance(analyzers, list):
        for index, item in enumerate(analyzers, start=1):
            if not isinstance(item, Mapping):
                continue
            name = str(item.get("name") or f"ga{index:02d}").strip()
            device_id = _normalize_device_id(item.get("device_id"))
            if name and device_id:
                mapping[_normalize_analyzer_label(name)] = device_id

    single = devices.get("gas_analyzer")
    if isinstance(single, Mapping):
        name = str(single.get("name") or "ga01").strip()
        device_id = _normalize_device_id(single.get("device_id"))
        if name and device_id:
            mapping.setdefault(_normalize_analyzer_label(name), device_id)

    return mapping


def _device_id_from_analyzer_label(label: Any, mapping: Mapping[str, str]) -> str:
    analyzer = str(label or "").strip()
    mapped = mapping.get(_normalize_analyzer_label(analyzer))
    if mapped:
        return mapped
    return _normalize_device_id(analyzer.replace("GA", "").replace("ga", "").strip())


def summarize(
    *,
    queue_run_dir: Path,
    output_dir: Path,
    target_devices: Iterable[str] = DEFAULT_TARGET_DEVICES,
    excluded_devices: Iterable[str] = DEFAULT_EXCLUDED_DEVICES,
    acceptance_pct: float = 1.0,
) -> dict[str, Any]:
    target_set = {_normalize_device_id(item) for item in target_devices if str(item).strip()}
    excluded_set = {_normalize_device_id(item) for item in excluded_devices if str(item).strip()}
    manifest = _read_csv(queue_run_dir / "queue_manifest.csv")
    queue_summary = _read_json(queue_run_dir / "queue_summary.json")
    rows: list[dict[str, Any]] = []

    for point in manifest:
        point_run_id = str(point.get("point_run_id") or "").strip()
        if not point_run_id:
            continue
        point_path = _point_dir(output_dir, point_run_id)
        analyzer_mapping = _analyzer_device_map(point_path)
        summary_path = _first_existing(point_path, ("分析仪汇总_*.csv", "*analyzer*summary*.csv"))
        if summary_path is None:
            continue
        for src in _read_csv(summary_path):
            analyzer = str(src.get("Analyzer") or "")
            device_id = _device_id_from_analyzer_label(analyzer, analyzer_mapping)
            target_ppm = _safe_float(src.get("ppm_CO2_Tank")) or _target_from_command(point)
            measured_ppm = _safe_float(src.get("ppm_CO2"))
            error_ppm = None
            error_pct = None
            if target_ppm not in (None, 0.0) and measured_ppm is not None:
                error_ppm = measured_ppm - target_ppm
                error_pct = error_ppm / target_ppm * 100.0
            status = "not_evaluated"
            if device_id in excluded_set:
                status = "excluded"
            elif device_id in target_set and error_pct is not None:
                status = "pass" if abs(error_pct) <= acceptance_pct else "fail"
            rows.append(
                {
                    "point_run_id": point_run_id,
                    "point_status": point.get("status", ""),
                    "source_nominal_ppm": _safe_float(point.get("source_nominal_ppm")),
                    "certificate_co2_ppm": target_ppm,
                    "co2_group": point.get("co2_group", ""),
                    "device_id": device_id,
                    "analyzer_label": analyzer,
                    "measured_co2_ppm": measured_ppm,
                    "error_ppm": error_ppm,
                    "error_pct": error_pct,
                    "acceptance_pct": acceptance_pct,
                    "status": status,
                    "valid_frames": _safe_float(src.get("ValidFrames")),
                    "total_frames": _safe_float(src.get("TotalFrames")),
                    "h2o_mmol_mol": _safe_float(src.get("ppm_H2O")),
                    "co2_ratio_f": _safe_float(src.get("R_CO2")),
                    "co2_ratio_f_dev": _safe_float(src.get("R_CO2_dev")),
                    "chamber_temp_c": _safe_float(src.get("T1")),
                    "case_temp_c": _safe_float(src.get("T2")),
                    "external_temp_c": _safe_float(src.get("Temp")),
                    "pressure_hpa": _safe_float(src.get("P")),
                    "summary_csv": str(summary_path.resolve()),
                }
            )

    target_rows = [row for row in rows if row.get("device_id") in target_set]
    failed_rows = [row for row in target_rows if row.get("status") != "pass"]
    point_ids = sorted({str(row.get("point_run_id")) for row in target_rows})
    devices_seen = sorted({str(row.get("device_id")) for row in target_rows})
    expected_pairs = len(point_ids) * len(target_set)
    observed_pairs = len(target_rows)
    overall_pass = (
        bool(point_ids)
        and not failed_rows
        and observed_pairs == expected_pairs
        and set(devices_seen) == target_set
        and bool(queue_summary.get("control_temperature"))
    )
    summary = {
        "schema_version": "v1_5_post_h2o_co2_verification_summary_v1",
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "queue_run_dir": str(queue_run_dir.resolve()),
        "output_dir": str(output_dir.resolve()),
        "target_devices": sorted(target_set),
        "excluded_devices": sorted(excluded_set),
        "acceptance_pct": acceptance_pct,
        "point_count": len(point_ids),
        "target_pair_count": observed_pairs,
        "expected_pair_count": expected_pairs,
        "failed_pair_count": len(failed_rows),
        "overall_pass": overall_pass,
        "control_temperature": bool(queue_summary.get("control_temperature")),
        "no_write": bool(queue_summary.get("no_write", True)),
        "sealed_pressure_control": bool(queue_summary.get("sealed_pressure_control")),
        "writes_senco": bool(queue_summary.get("writes_senco")),
        "writes_device_id": bool(queue_summary.get("writes_device_id")),
    }
    return {"summary": summary, "rows": rows}


def write_outputs(payload: Mapping[str, Any], output_dir: Path, prefix: str) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    summary = dict(payload.get("summary") or {})
    rows = list(payload.get("rows") or [])
    csv_path = output_dir / f"{prefix}_per_device.csv"
    json_path = output_dir / f"{prefix}.json"
    md_path = output_dir / f"{prefix}.md"
    _write_csv(csv_path, rows)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")

    lines = [
        "# V1.5 Post-H2O CO2 Verification Summary",
        "",
        f"- Overall pass: `{summary.get('overall_pass')}`",
        f"- Acceptance: `±{summary.get('acceptance_pct')}%`",
        f"- Target devices: `{', '.join(summary.get('target_devices') or [])}`",
        f"- Points: `{summary.get('point_count')}`",
        f"- Failed target pairs: `{summary.get('failed_pair_count')}`",
        f"- Control temperature: `{summary.get('control_temperature')}`",
        f"- No write: `{summary.get('no_write')}`",
        "",
        "| Point | Device | Cert ppm | Measured ppm | Error % | Status |",
        "| --- | --- | ---: | ---: | ---: | --- |",
    ]
    lines[3] = f"- Acceptance: `±{summary.get('acceptance_pct')}%`"
    lines[3] = f"- Acceptance: `+/-{summary.get('acceptance_pct')}%`"
    for row in rows:
        status = row.get("status")
        if status not in {"pass", "fail"}:
            continue
        lines.append(
            "| {point} | {dev} | {cert:.3f} | {meas:.3f} | {err:.3f} | {status} |".format(
                point=row.get("point_run_id", ""),
                dev=row.get("device_id", ""),
                cert=float(row.get("certificate_co2_ppm") or 0.0),
                meas=float(row.get("measured_co2_ppm") or 0.0),
                err=float(row.get("error_pct") or 0.0),
                status=status,
            )
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"csv": csv_path, "json": json_path, "markdown": md_path}


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize V1.5 post-H2O CO2 verification evidence.")
    parser.add_argument("--queue-run-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--point-output-dir", default=None)
    parser.add_argument("--target-devices", default="022,030,033,051")
    parser.add_argument("--excluded-devices", default="")
    parser.add_argument("--acceptance-pct", type=float, default=1.0)
    parser.add_argument("--prefix", default="co2_post_h2o_verification_summary")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    queue_run_dir = Path(args.queue_run_dir).resolve()
    point_output_dir = Path(args.point_output_dir).resolve() if args.point_output_dir else queue_run_dir.parent.resolve()
    output_dir = Path(args.output_dir).resolve()
    payload = summarize(
        queue_run_dir=queue_run_dir,
        output_dir=point_output_dir,
        target_devices=[item.strip() for item in args.target_devices.split(",") if item.strip()],
        excluded_devices=[item.strip() for item in args.excluded_devices.split(",") if item.strip()],
        acceptance_pct=float(args.acceptance_pct),
    )
    outputs = write_outputs(payload, output_dir, args.prefix)
    print(json.dumps({key: str(value) for key, value in outputs.items()}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
