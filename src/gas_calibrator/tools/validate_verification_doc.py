"""Open-route validation for four gas analyzers and Word-form fill-in."""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import statistics
import time
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from xml.etree import ElementTree as ET

from ..config import load_config
from ..devices import DewpointMeter, GasAnalyzer, HumidityGenerator, ParoscientificGauge, RelayController
from .run_gas_route_ratio_leak_check import _apply_logical_valves, _route_for_gas_ppm

WORD_NS = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
ET.register_namespace("w", WORD_NS["w"])

DEFAULT_DEVICE_IDS = {"025", "006", "009", "017"}
DEFAULT_FLOW_LPM = 1.5


def _log(message: str) -> None:
    print(message, flush=True)


def _as_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except Exception:
        return None


def _first_float(text: Any) -> Optional[float]:
    match = re.search(r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)", str(text or ""))
    return None if not match else float(match.group(0))


def _mean(values: Sequence[Optional[float]]) -> Optional[float]:
    clean = [float(value) for value in values if value is not None]
    return statistics.fmean(clean) if clean else None


def _stdev(values: Sequence[Optional[float]]) -> Optional[float]:
    clean = [float(value) for value in values if value is not None]
    return statistics.stdev(clean) if len(clean) > 1 else None


def _format_value(value: Optional[float], unit: str, digits: int = 3) -> str:
    return "--" if value is None else f"{float(value):.{digits}f} {unit}"


def _format_error(value: Optional[float], unit: str, digits: int = 3) -> str:
    return "--" if value is None else f"{float(value):+.{digits}f} {unit}"


def _format_rel(reference: Optional[float], measured: Optional[float], digits: int = 3) -> str:
    if reference in (None, 0) or measured is None:
        return "--"
    relative = 100.0 * (float(measured) - float(reference)) / float(reference)
    return f"{relative:+.{digits}f} %"


def _error(reference: Optional[float], measured: Optional[float]) -> Optional[float]:
    if reference is None or measured is None:
        return None
    return float(measured) - float(reference)


def _sat_hpa(temp_c: float) -> float:
    temp = float(temp_c)
    if temp >= 0.0:
        return 6.1121 * math.exp((18.678 - temp / 234.5) * (temp / (257.14 + temp)))
    return 6.1115 * math.exp((23.036 - temp / 333.7) * (temp / (279.82 + temp)))


def dewpoint_to_h2o_mmol_per_mol(dewpoint_c: float, pressure_hpa: float) -> float:
    pressure = float(pressure_hpa)
    if pressure <= 0:
        raise ValueError("pressure_hpa must be positive")
    return 1000.0 * _sat_hpa(float(dewpoint_c)) / pressure


def derive_humidity_generator_setpoint(dewpoint_c: float) -> Dict[str, float]:
    dewpoint = float(dewpoint_c)
    hgen_temp_c = max(20.0, math.ceil((dewpoint + 5.0) / 5.0) * 5.0)
    hgen_rh_pct = min(95.0, 100.0 * _sat_hpa(dewpoint) / _sat_hpa(hgen_temp_c))
    return {"hgen_temp_c": round(hgen_temp_c, 3), "hgen_rh_pct": round(hgen_rh_pct, 3)}


def _cell_text(cell: ET.Element) -> str:
    return "".join(node.text or "" for node in cell.findall(".//w:t", WORD_NS)).strip()


def _set_cell_text(cell: ET.Element, text: str) -> None:
    text_nodes = cell.findall(".//w:t", WORD_NS)
    if not text_nodes:
        paragraph = cell.find(".//w:p", WORD_NS)
        if paragraph is None:
            paragraph = ET.SubElement(cell, f"{{{WORD_NS['w']}}}p")
        run = ET.SubElement(paragraph, f"{{{WORD_NS['w']}}}r")
        text_node = ET.SubElement(run, f"{{{WORD_NS['w']}}}t")
        text_node.text = str(text)
        return
    text_nodes[0].text = str(text)
    for node in text_nodes[1:]:
        node.text = ""


def _table_rows(root: ET.Element) -> List[List[ET.Element]]:
    table = root.find(".//w:tbl", WORD_NS)
    if table is None:
        raise ValueError("template has no table")
    return [list(row.findall("w:tc", WORD_NS)) for row in table.findall("w:tr", WORD_NS)]


def load_template_spec(template_path: Path) -> Dict[str, Any]:
    with zipfile.ZipFile(template_path) as archive:
        root = ET.fromstring(archive.read("word/document.xml"))
    co2_rows: List[Dict[str, Any]] = []
    h2o_rows: List[Dict[str, Any]] = []
    for row_index, cells in enumerate(_table_rows(root)):
        texts = [_cell_text(cell) for cell in cells]
        if len(texts) < 6:
            continue
        point_text = texts[1]
        if point_text in {"0", "200", "400", "600", "800", "1000"}:
            co2_rows.append(
                {
                    "row_index": row_index,
                    "nominal_ppm": int(point_text),
                    "standard_ppm": _first_float(texts[2]),
                }
            )
        elif "露点温度" in point_text:
            h2o_rows.append(
                {
                    "row_index": row_index,
                    "target_dewpoint_c": _first_float(point_text),
                }
            )
    if len(co2_rows) != 6 or len(h2o_rows) != 2:
        raise ValueError("template rows not recognized")
    return {"co2_rows": co2_rows, "h2o_rows": h2o_rows}


def write_filled_docx(
    template_path: Path,
    output_path: Path,
    *,
    co2_rows: Sequence[Mapping[str, Any]],
    h2o_rows: Sequence[Mapping[str, Any]],
) -> None:
    with zipfile.ZipFile(template_path, "r") as archive:
        file_map = {name: archive.read(name) for name in archive.namelist()}
    root = ET.fromstring(file_map["word/document.xml"])
    rows = _table_rows(root)
    for row in co2_rows:
        cells = rows[int(row["row_index"])]
        _set_cell_text(cells[3], str(row["sensor_text"]))
        _set_cell_text(cells[4], str(row["error_text"]))
        _set_cell_text(cells[5], str(row["relative_text"]))
    for row in h2o_rows:
        cells = rows[int(row["row_index"])]
        _set_cell_text(cells[2], str(row["standard_text"]))
        _set_cell_text(cells[3], str(row["sensor_text"]))
        _set_cell_text(cells[4], str(row["error_text"]))
        _set_cell_text(cells[5], str(row["relative_text"]))
    file_map["word/document.xml"] = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    with zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, payload in file_map.items():
            archive.writestr(name, payload)


def _load_targets(cfg: Mapping[str, Any], targets_json: Optional[str]) -> List[Dict[str, Any]]:
    if targets_json:
        payload = json.loads(Path(targets_json).read_text(encoding="utf-8"))
        targets = payload.get("devices", {}).get("gas_analyzers", [])
    else:
        targets = cfg.get("devices", {}).get("gas_analyzers", [])
    rows: List[Dict[str, Any]] = []
    for row in targets:
        if not isinstance(row, Mapping) or not row.get("enabled", True):
            continue
        device_id = GasAnalyzer.normalize_device_id(row.get("device_id") or "000")
        if device_id not in DEFAULT_DEVICE_IDS:
            continue
        rows.append(
            {
                "name": str(row.get("name") or "").strip() or f"ga{len(rows) + 1:02d}",
                "port": str(row.get("port") or "").strip(),
                "baud": int(row.get("baud", 115200) or 115200),
                "device_id": device_id,
            }
        )
    return rows


def _build_devices(
    cfg: Mapping[str, Any],
    targets: Sequence[Mapping[str, Any]],
    *,
    include_h2o_devices: bool,
) -> Dict[str, Any]:
    devices_cfg = cfg.get("devices", {})
    built: Dict[str, Any] = {"analyzers": []}
    try:
        for target in targets:
            analyzer = GasAnalyzer(target["port"], target["baud"], timeout=0.5, device_id=target["device_id"])
            analyzer.open()
            analyzer.active_send = True
            built["analyzers"].append({"meta": dict(target), "device": analyzer})
        if include_h2o_devices:
            dew_cfg = devices_cfg.get("dewpoint_meter", {})
            built["dewpoint"] = DewpointMeter(
                dew_cfg["port"],
                dew_cfg.get("baud", 9600),
                station=str(dew_cfg.get("station", "001")),
            )
            built["dewpoint"].open()
            hgen_cfg = devices_cfg.get("humidity_generator", {})
            built["humidity_generator"] = HumidityGenerator(hgen_cfg["port"], hgen_cfg.get("baud", 9600))
            built["humidity_generator"].open()
            gauge_cfg = devices_cfg.get("pressure_gauge", {})
            built["pressure_gauge"] = ParoscientificGauge(
                gauge_cfg["port"],
                gauge_cfg.get("baud", 9600),
                dest_id=str(gauge_cfg.get("dest_id", "01")),
                response_timeout_s=float(gauge_cfg.get("response_timeout_s", 1.5) or 1.5),
            )
            built["pressure_gauge"].open()
        relay_cfg = devices_cfg.get("relay", {})
        built["relay"] = RelayController(relay_cfg["port"], relay_cfg.get("baud", 38400), addr=relay_cfg.get("addr", 1))
        built["relay"].open()
        relay_8_cfg = devices_cfg.get("relay_8", {})
        built["relay_8"] = RelayController(
            relay_8_cfg["port"],
            relay_8_cfg.get("baud", 38400),
            addr=relay_8_cfg.get("addr", 1),
        )
        built["relay_8"].open()
        return built
    except Exception:
        close_devices(built)
        raise


def close_devices(devices: Mapping[str, Any]) -> None:
    seen: set[int] = set()
    for value in devices.values():
        items = [entry.get("device") for entry in value] if isinstance(value, list) else [value]
        for item in items:
            if item is None or not hasattr(item, "close") or id(item) in seen:
                continue
            seen.add(id(item))
            try:
                item.close()
            except Exception:
                pass


def _collect_sample(analyzer: GasAnalyzer) -> Dict[str, Any]:
    raw = analyzer.read_latest_data(prefer_stream=True, drain_s=0.2, read_timeout_s=0.05, allow_passive_fallback=True)
    parsed = analyzer.parse_line(raw) if raw else None
    mode = "unknown"
    if isinstance(parsed, Mapping):
        mode = "mode2" if int(parsed.get("mode", 0) or 0) == 2 else "mode1"
    pressure_kpa = _as_float(parsed.get("pressure_kpa")) if isinstance(parsed, Mapping) else None
    return {
        "raw": raw,
        "mode": mode,
        "co2_ppm": _as_float(parsed.get("co2_ppm")) if isinstance(parsed, Mapping) else None,
        "h2o_mmol": _as_float(parsed.get("h2o_mmol")) if isinstance(parsed, Mapping) else None,
        "pressure_hpa": None if pressure_kpa is None else pressure_kpa * 10.0,
    }


def probe_modes(analyzers: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for entry in analyzers:
        samples = []
        for _ in range(6):
            samples.append(_collect_sample(entry["device"]))
            time.sleep(0.12)
        mode_counts: Dict[str, int] = {}
        for sample in samples:
            mode_counts[sample["mode"]] = mode_counts.get(sample["mode"], 0) + 1
        rows.append({**entry["meta"], "best_mode": max(mode_counts, key=mode_counts.get), "mode_counts": mode_counts})
    return rows


def _collect_window(
    analyzers: Sequence[Mapping[str, Any]],
    *,
    duration_s: float,
    sample_interval_s: float,
    dewpoint: Optional[DewpointMeter] = None,
    pressure_gauge: Optional[ParoscientificGauge] = None,
) -> Dict[str, Any]:
    started = time.monotonic()
    data = {"analyzers": {}, "dewpoint": [], "pressure": []}
    for entry in analyzers:
        data["analyzers"][entry["meta"]["name"]] = []
    while True:
        elapsed = time.monotonic() - started
        if elapsed >= duration_s and any(data["analyzers"].values()):
            return data
        loop_started = time.monotonic()
        if dewpoint is not None:
            frame = dewpoint.get_current_fast(timeout_s=0.4, clear_buffer=False)
            data["dewpoint"].append(
                {
                    "dewpoint_c": _as_float(frame.get("dewpoint_c")),
                    "temp_c": _as_float(frame.get("temp_c")),
                    "rh_pct": _as_float(frame.get("rh_pct")),
                }
            )
        if pressure_gauge is not None:
            data["pressure"].append({"pressure_hpa": float(pressure_gauge.read_pressure_fast(retries=1, buffered_drain_s=0.05))})
        for entry in analyzers:
            data["analyzers"][entry["meta"]["name"]].append(_collect_sample(entry["device"]))
        wait_s = max(0.0, float(sample_interval_s) - (time.monotonic() - loop_started))
        if wait_s > 0:
            time.sleep(wait_s)


def _summarize(samples: Sequence[Mapping[str, Any]], key: str) -> Dict[str, Any]:
    values = [_as_float(sample.get(key)) for sample in samples]
    mode_counts: Dict[str, int] = {}
    for sample in samples:
        mode = str(sample.get("mode") or "unknown")
        mode_counts[mode] = mode_counts.get(mode, 0) + 1
    return {"mean": _mean(values), "stdev": _stdev(values), "count": len(samples), "mode_counts": mode_counts}


def _wait_dewpoint_ready(dewpoint: DewpointMeter, target_c: float, timeout_s: float) -> Dict[str, Any]:
    started = time.monotonic()
    history: List[Tuple[float, float]] = []
    while True:
        frame = dewpoint.get_current_fast(timeout_s=0.4, clear_buffer=False)
        value = _as_float(frame.get("dewpoint_c"))
        now = time.monotonic()
        if value is not None:
            history.append((now, value))
        history = [(ts, one) for ts, one in history if now - ts <= 60.0]
        values = [one for _, one in history]
        span_c = max(values) - min(values) if len(values) >= 2 else None
        if value is not None and abs(value - float(target_c)) <= 0.4 and span_c is not None and span_c <= 0.15 and len(values) >= 4:
            return {"ok": True, "elapsed_s": round(now - started, 3), "dewpoint_c": value, "span_c": span_c}
        if now - started >= timeout_s:
            return {"ok": False, "elapsed_s": round(now - started, 3), "dewpoint_c": value, "span_c": span_c}
        time.sleep(1.0)


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    header: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in header:
                header.append(str(key))
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row))


def _read_csv_rows(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _build_reused_h2o_rows(
    spec: Mapping[str, Any],
    summary_csv: Path,
) -> Tuple[Dict[str, List[Dict[str, Any]]], List[Dict[str, Any]]]:
    rows = _read_csv_rows(summary_csv)
    h2o_row_index = {
        round(float(item["target_dewpoint_c"]), 3): int(item["row_index"])
        for item in spec["h2o_rows"]
        if item.get("target_dewpoint_c") is not None
    }
    report_rows: Dict[str, List[Dict[str, Any]]] = {}
    summary_rows: List[Dict[str, Any]] = []
    for row in rows:
        if str(row.get("point_type") or "").strip().lower() != "h2o":
            continue
        target_dewpoint_c = _as_float(row.get("point"))
        if target_dewpoint_c is None:
            continue
        row_index = h2o_row_index.get(round(float(target_dewpoint_c), 3))
        if row_index is None:
            continue
        analyzer = str(row.get("analyzer") or "").strip().upper()
        reference = _as_float(row.get("reference_value"))
        measured = _as_float(row.get("measured_value"))
        error = _as_float(row.get("error_value"))
        report_rows.setdefault(analyzer, []).append(
            {
                "row_index": row_index,
                "standard_text": _format_value(reference, "mmol/mol"),
                "sensor_text": _format_value(measured, "mmol/mol"),
                "error_text": _format_error(error, "mmol/mol"),
                "relative_text": str(row.get("relative_error_text") or _format_rel(reference, measured)),
            }
        )
        summary_rows.append(dict(row))
    return report_rows, summary_rows


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate four analyzers at ambient pressure and fill Word forms.")
    parser.add_argument("--config", default="configs/default_config.json")
    parser.add_argument("--template", required=True)
    parser.add_argument("--targets-json", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--co2-settle-s", type=float, default=18.0)
    parser.add_argument("--co2-sample-s", type=float, default=8.0)
    parser.add_argument("--h2o-timeout-s", type=float, default=900.0)
    parser.add_argument("--h2o-sample-s", type=float, default=12.0)
    parser.add_argument("--sample-interval-s", type=float, default=1.0)
    parser.add_argument("--flow-lpm", type=float, default=DEFAULT_FLOW_LPM)
    parser.add_argument("--skip-h2o", action="store_true")
    parser.add_argument("--reuse-summary-csv", default=None)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    cfg = load_config(args.config)
    template_path = Path(args.template).resolve()
    output_dir = (
        Path(args.output_dir).resolve()
        if args.output_dir
        else Path(cfg.get("paths", {}).get("output_dir", "logs")).resolve() / f"verify_doc_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    spec = load_template_spec(template_path)
    targets = _load_targets(cfg, args.targets_json)
    if len(targets) != 4:
        raise ValueError(f"need 4 targets, got {len(targets)}")
    if args.skip_h2o and not args.reuse_summary_csv:
        raise ValueError("--skip-h2o requires --reuse-summary-csv so the output form keeps the previous H2O rows")
    devices = _build_devices(cfg, targets, include_h2o_devices=not args.skip_h2o)
    report_rows: Dict[str, Dict[str, List[Dict[str, Any]]]] = {row["name"].upper(): {"co2": [], "h2o": []} for row in targets}
    summary_rows: List[Dict[str, Any]] = []
    raw_rows: List[Dict[str, Any]] = []
    h2o_meta: List[Dict[str, Any]] = []
    try:
        mode_probe = probe_modes(devices["analyzers"])
        (output_dir / "analyzer_mode_probe.json").write_text(json.dumps(mode_probe, ensure_ascii=False, indent=2), encoding="utf-8")
        _log("Mode probe: " + ", ".join(f"{row['name']}={row['best_mode']}" for row in mode_probe))

        for point in spec["co2_rows"]:
            route = _route_for_gas_ppm(cfg, int(point["nominal_ppm"]), "A")
            _apply_logical_valves(cfg, devices, route["open_logical_valves"])
            _log(f"CO2 point {point['nominal_ppm']} ppm -> open {route['open_logical_valves']}")
            time.sleep(float(args.co2_settle_s))
            window = _collect_window(devices["analyzers"], duration_s=float(args.co2_sample_s), sample_interval_s=float(args.sample_interval_s))
            for analyzer_name, samples in window["analyzers"].items():
                for sample in samples:
                    raw_rows.append({"point": f"co2_{point['nominal_ppm']}", "analyzer": analyzer_name, **sample})
                stats = _summarize(samples, "co2_ppm")
                measured = stats["mean"]
                error = _error(point["standard_ppm"], measured)
                record = {
                    "row_index": point["row_index"],
                    "sensor_text": _format_value(measured, "ppm"),
                    "error_text": _format_error(error, "ppm"),
                    "relative_text": _format_rel(point["standard_ppm"], measured),
                }
                report_rows[analyzer_name.upper()]["co2"].append(record)
                summary_rows.append(
                    {
                        "analyzer": analyzer_name.upper(),
                        "device_id": next(row["device_id"] for row in targets if row["name"] == analyzer_name),
                        "point_type": "co2",
                        "point": point["nominal_ppm"],
                        "reference_value": point["standard_ppm"],
                        "measured_value": measured,
                        "error_value": error,
                        "relative_error_text": record["relative_text"],
                        "sample_count": stats["count"],
                        "mode_counts": json.dumps(stats["mode_counts"], ensure_ascii=False),
                        "stdev": stats["stdev"],
                    }
                )

        if args.skip_h2o:
            reused_h2o_rows, reused_summary_rows = _build_reused_h2o_rows(spec, Path(args.reuse_summary_csv).resolve())
            for analyzer, rows in reused_h2o_rows.items():
                report_rows.setdefault(analyzer, {"co2": [], "h2o": []})
                report_rows[analyzer]["h2o"] = list(rows)
            summary_rows.extend(reused_summary_rows)
            reuse_meta_path = Path(args.reuse_summary_csv).resolve().with_name("h2o_reference_summary.json")
            if reuse_meta_path.exists():
                h2o_meta = json.loads(reuse_meta_path.read_text(encoding="utf-8"))
        else:
            h2o_open = [int(cfg["valves"][key]) for key in ("h2o_path", "hold", "flow_switch")]
            for point in spec["h2o_rows"]:
                target_dewpoint_c = float(point["target_dewpoint_c"])
                setpoint = derive_humidity_generator_setpoint(target_dewpoint_c)
                hgen = devices["humidity_generator"]
                hgen.set_target_temp(setpoint["hgen_temp_c"])
                hgen.set_target_rh(setpoint["hgen_rh_pct"])
                hgen.set_flow_target(float(args.flow_lpm))
                hgen.enable_control(True)
                try:
                    hgen.heat_on()
                except Exception:
                    pass
                try:
                    hgen.cool_on()
                except Exception:
                    pass
                hgen.ensure_run(min_flow_lpm=max(0.1, float(args.flow_lpm) * 0.25), tries=3, wait_s=4.0, poll_s=0.4)
                _apply_logical_valves(cfg, devices, h2o_open)
                wait_state = _wait_dewpoint_ready(devices["dewpoint"], target_dewpoint_c, float(args.h2o_timeout_s))
                _log(f"H2O point dewpoint={target_dewpoint_c:.1f}C -> wait={wait_state}")
                window = _collect_window(
                    devices["analyzers"],
                    duration_s=float(args.h2o_sample_s),
                    sample_interval_s=float(args.sample_interval_s),
                    dewpoint=devices["dewpoint"],
                    pressure_gauge=devices["pressure_gauge"],
                )
                ref_dewpoint_c = _mean([row.get("dewpoint_c") for row in window["dewpoint"]])
                ref_pressure_hpa = _mean([row.get("pressure_hpa") for row in window["pressure"]])
                ref_mmol = None if ref_dewpoint_c is None or ref_pressure_hpa is None else dewpoint_to_h2o_mmol_per_mol(ref_dewpoint_c, ref_pressure_hpa)
                h2o_meta.append(
                    {
                        "target_dewpoint_c": target_dewpoint_c,
                        "hgen_temp_c": setpoint["hgen_temp_c"],
                        "hgen_rh_pct": setpoint["hgen_rh_pct"],
                        "wait_ok": wait_state["ok"],
                        "reference_dewpoint_c": ref_dewpoint_c,
                        "reference_pressure_hpa": ref_pressure_hpa,
                        "reference_mmol_per_mol": ref_mmol,
                    }
                )
                for analyzer_name, samples in window["analyzers"].items():
                    for sample in samples:
                        raw_rows.append({"point": f"h2o_{target_dewpoint_c:g}", "analyzer": analyzer_name, **sample})
                    stats = _summarize(samples, "h2o_mmol")
                    measured = stats["mean"]
                    error = _error(ref_mmol, measured)
                    record = {
                        "row_index": point["row_index"],
                        "standard_text": _format_value(ref_mmol, "mmol/mol"),
                        "sensor_text": _format_value(measured, "mmol/mol"),
                        "error_text": _format_error(error, "mmol/mol"),
                        "relative_text": _format_rel(ref_mmol, measured),
                    }
                    report_rows[analyzer_name.upper()]["h2o"].append(record)
                    summary_rows.append(
                        {
                            "analyzer": analyzer_name.upper(),
                            "device_id": next(row["device_id"] for row in targets if row["name"] == analyzer_name),
                            "point_type": "h2o",
                            "point": target_dewpoint_c,
                            "reference_value": ref_mmol,
                            "reference_dewpoint_c": ref_dewpoint_c,
                            "reference_pressure_hpa": ref_pressure_hpa,
                            "measured_value": measured,
                            "error_value": error,
                            "relative_error_text": record["relative_text"],
                            "sample_count": stats["count"],
                            "mode_counts": json.dumps(stats["mode_counts"], ensure_ascii=False),
                            "stdev": stats["stdev"],
                            "hgen_temp_c": setpoint["hgen_temp_c"],
                            "hgen_rh_pct": setpoint["hgen_rh_pct"],
                            "wait_ok": wait_state["ok"],
                        }
                    )

        _apply_logical_valves(cfg, devices, [])
        if "humidity_generator" in devices:
            devices["humidity_generator"].safe_stop()
        _write_csv(output_dir / "validation_summary.csv", summary_rows)
        _write_csv(output_dir / "raw_samples.csv", raw_rows)
        (output_dir / "h2o_reference_summary.json").write_text(json.dumps(h2o_meta, ensure_ascii=False, indent=2), encoding="utf-8")
        for target in targets:
            output_path = output_dir / f"{template_path.stem}_{target['name'].upper()}_{target['device_id']}.docx"
            write_filled_docx(
                template_path,
                output_path,
                co2_rows=report_rows[target["name"].upper()]["co2"],
                h2o_rows=report_rows[target["name"].upper()]["h2o"],
            )
        (output_dir / "run_summary.json").write_text(
            json.dumps({"template": str(template_path), "targets": targets, "mode_probe": mode_probe}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return 0
    finally:
        try:
            _apply_logical_valves(cfg, devices, [])
        except Exception:
            pass
        try:
            if "humidity_generator" in devices:
                devices["humidity_generator"].safe_stop()
        except Exception:
            pass
        close_devices(devices)


if __name__ == "__main__":
    raise SystemExit(main())
