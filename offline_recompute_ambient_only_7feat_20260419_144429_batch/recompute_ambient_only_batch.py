from __future__ import annotations

import csv
import json
import shutil
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from gas_calibrator.tools import run_v1_corrected_autodelivery as autodelivery

ROOT = Path(r"D:/gas_calibrator")
SUMMARY_ROOT = Path(r"D:/gas_calibrator/offline_recompute_ambient_only_7feat_20260419_144429_batch")
FORCED_MODEL_FEATURES = ["intercept", "R", "R2", "R3", "T", "T2", "RT"]
RUN_STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
GAS_PREFIX = "\u5206\u6790\u4eea\u6c47\u603b_\u6c14\u8def_"
WATER_PREFIX = "\u5206\u6790\u4eea\u6c47\u603b_\u6c34\u8def_"
COMBINED_PREFIX = "\u5206\u6790\u4eea\u6c47\u603b_"

ORIG_INSERT = pd.DataFrame.insert

def patched_insert(self, loc, column, value, allow_duplicates=False):
    if column in self.columns and not allow_duplicates:
        self.drop(columns=[column], inplace=True)
    return ORIG_INSERT(self, loc, column, value, allow_duplicates=allow_duplicates)

pd.DataFrame.insert = patched_insert


def run_git(*args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True, encoding="utf-8").strip()


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def startswith_latest(run_dir: Path, prefix: str) -> Path | None:
    matches = [path for path in run_dir.iterdir() if path.is_file() and path.name.startswith(prefix)]
    return max(matches, key=lambda item: item.stat().st_mtime) if matches else None


def resolve_summary_sources(run_dir: Path) -> dict[str, Path | None]:
    return {
        "gas": startswith_latest(run_dir, GAS_PREFIX),
        "water": startswith_latest(run_dir, WATER_PREFIX),
        "combined": startswith_latest(run_dir, COMBINED_PREFIX),
    }


def resolve_samples_path(run_dir: Path) -> Path | None:
    matches = [path for path in run_dir.glob("samples_*.csv") if path.is_file()]
    if matches:
        return max(matches, key=lambda item: item.stat().st_mtime)
    fallback = run_dir / "samples.csv"
    return fallback if fallback.exists() else None


def normalize_selected_pressure_points(raw: Any) -> list[str]:
    if raw in (None, "", []):
        return []
    values = raw if isinstance(raw, list) else [raw]
    return [str(item).strip().lower() for item in values if str(item).strip()]


def phase_key(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"??", "co2"}:
        return "co2"
    if text in {"??", "h2o"}:
        return "h2o"
    return text


def infer_ambient_only_from_files(run_dir: Path) -> tuple[bool, str]:
    samples_path = resolve_samples_path(run_dir)
    if samples_path is not None:
        try:
            with samples_path.open("r", encoding="utf-8-sig", newline="") as handle:
                rows = list(csv.DictReader(handle))
            for column in ("??????", "PressureMode"):
                if rows and column in rows[0]:
                    modes = [str(row.get(column) or "").strip().lower() for row in rows if str(row.get(column) or "").strip()]
                    if modes:
                        if any("sealed" in mode for mode in modes):
                            return False, f"samples_pressure_modes={sorted(set(modes))}"
                        if all("ambient" in mode or mode == "open" for mode in modes):
                            return True, f"samples_pressure_modes={sorted(set(modes))}"
        except Exception as exc:
            return False, f"samples_inspection_error:{exc}"
    point_files = [path.name.lower() for path in run_dir.glob("point_*_samples.csv") if path.is_file()]
    if point_files:
        if all("ambient" in name for name in point_files):
            return True, "point_sample_filenames_all_ambient"
        return False, "point_sample_filenames_include_non_ambient"
    return False, "no_confirming_ambient_evidence"


def classify_run(run_dir: Path) -> dict[str, Any]:
    snapshot_path = run_dir / "runtime_config_snapshot.json"
    try:
        cfg = load_json(snapshot_path)
    except Exception as exc:
        return {"status": "skipped", "reason": f"runtime_snapshot_unreadable:{exc}", "ambient_only": False}
    workflow_cfg = dict(cfg.get("workflow") or {}) if isinstance(cfg, dict) else {}
    selected = normalize_selected_pressure_points(workflow_cfg.get("selected_pressure_points"))
    summaries = resolve_summary_sources(run_dir)
    samples_path = resolve_samples_path(run_dir)
    ambient_only = False
    ambient_reason = ""
    if selected:
        ambient_only = all(item == "ambient" for item in selected)
        ambient_reason = f"workflow.selected_pressure_points={selected}"
        if not ambient_only:
            return {
                "status": "skipped",
                "reason": f"not_ambient_only:{ambient_reason}",
                "ambient_only": False,
                "selected_pressure_points": selected,
                "summary_sources": {key: str(value) if value else "" for key, value in summaries.items()},
                "samples_path": str(samples_path) if samples_path else "",
            }
    else:
        ambient_only, ambient_reason = infer_ambient_only_from_files(run_dir)
        if not ambient_only:
            return {
                "status": "skipped",
                "reason": f"not_confirmed_ambient_only:{ambient_reason}",
                "ambient_only": False,
                "selected_pressure_points": selected,
                "summary_sources": {key: str(value) if value else "" for key, value in summaries.items()},
                "samples_path": str(samples_path) if samples_path else "",
            }
    missing = []
    if summaries["gas"] is None and summaries["combined"] is None:
        missing.append("gas_or_combined_summary")
    if summaries["water"] is None and summaries["combined"] is None:
        missing.append("water_or_combined_summary")
    if samples_path is None:
        missing.append("samples_csv")
    if missing:
        return {
            "status": "skipped",
            "reason": f"missing_required_files:{','.join(missing)}",
            "ambient_only": ambient_only,
            "ambient_reason": ambient_reason,
            "selected_pressure_points": selected,
            "summary_sources": {key: str(value) if value else "" for key, value in summaries.items()},
            "samples_path": str(samples_path) if samples_path else "",
        }
    return {
        "status": "ready",
        "ambient_only": True,
        "ambient_reason": ambient_reason,
        "selected_pressure_points": selected,
        "summary_sources": {key: str(value) if value else "" for key, value in summaries.items()},
        "samples_path": str(samples_path),
        "cfg": cfg,
    }


def load_frame(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path, encoding="utf-8-sig")
    sheets = pd.read_excel(path, sheet_name=None)
    rows = []
    for sheet, frame in sheets.items():
        payload = frame.copy()
        if "Analyzer" not in payload.columns:
            payload["Analyzer"] = sheet
        rows.append(payload)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def write_workbook(frame: pd.DataFrame, path: Path) -> None:
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        analyzers = sorted(str(value).strip() for value in frame.get("Analyzer", pd.Series(dtype=str)).dropna().unique())
        for analyzer in analyzers:
            group = frame[frame["Analyzer"].astype(str).eq(analyzer)].copy()
            if "Analyzer" in group.columns:
                group = group.drop(columns=["Analyzer"])
            group.to_excel(writer, sheet_name=analyzer[:31], index=False)


def build_staging_run(run_dir: Path, output_dir: Path, classification: dict[str, Any]) -> tuple[Path, Path]:
    staging_dir = output_dir / "_staging_run"
    staging_dir.mkdir(parents=True, exist_ok=True)
    cfg = dict(classification["cfg"])
    coeff_cfg = dict(cfg.get("coefficients") or {})
    coeff_cfg["model_features"] = list(FORCED_MODEL_FEATURES)
    cfg["coefficients"] = coeff_cfg
    if not classification.get("selected_pressure_points"):
        workflow_cfg = dict(cfg.get("workflow") or {})
        workflow_cfg["selected_pressure_points"] = ["ambient"]
        cfg["workflow"] = workflow_cfg
    temp_cfg_path = output_dir / "runtime_config_snapshot_recompute.json"
    write_json(temp_cfg_path, cfg)

    summaries = {key: Path(value) for key, value in classification["summary_sources"].items() if value}
    combined_df = load_frame(summaries["combined"]) if "combined" in summaries else pd.DataFrame()
    gas_df = load_frame(summaries["gas"]) if "gas" in summaries else combined_df[combined_df["PointPhase"].map(phase_key).eq("co2")].copy()
    water_df = load_frame(summaries["water"]) if "water" in summaries else combined_df[combined_df["PointPhase"].map(phase_key).eq("h2o")].copy()
    if gas_df.empty:
        raise RuntimeError("staging_gas_summary_empty")
    if water_df.empty:
        raise RuntimeError("staging_water_summary_empty")
    write_workbook(gas_df, staging_dir / f"{GAS_PREFIX}{run_dir.name}.xlsx")
    write_workbook(water_df, staging_dir / f"{WATER_PREFIX}{run_dir.name}.xlsx")

    samples_path = Path(classification["samples_path"])
    shutil.copy2(samples_path, staging_dir / samples_path.name)
    shutil.copy2(run_dir / "runtime_config_snapshot.json", staging_dir / "runtime_config_snapshot.json")
    for name in [
        "temperature_compensation_coefficients.csv",
        "temperature_compensation.xlsx",
        "temperature_compensation_commands.txt",
    ]:
        source = run_dir / name
        if source.exists():
            shutil.copy2(source, staging_dir / name)
    return staging_dir, temp_cfg_path


def find_old_ratio_poly_json(run_dir: Path, gas: str, analyzer: str) -> Path | None:
    pattern = f"{gas.lower()}_{analyzer.upper()}_ratio_poly_fit_"
    matches = [path for path in run_dir.iterdir() if path.is_file() and path.name.startswith(pattern) and path.suffix.lower() == ".json"]
    return max(matches, key=lambda item: item.stat().st_mtime) if matches else None


def old_row_from_json(path: Path, analyzer: str, gas: str) -> dict[str, Any]:
    payload = load_json(path)
    coeffs = dict(payload.get("simplified_coefficients") or {})
    stats = dict(payload.get("stats") or {})
    out = {
        "Analyzer": analyzer.upper(),
        "Gas": gas.upper(),
        "old_model_features": ",".join(list(stats.get("model_features") or [])),
        "old_source": str(path),
    }
    for idx in range(9):
        out[f"old_a{idx}"] = coeffs.get(f"a{idx}")
    return out


def load_old_rows(run_dir: Path) -> dict[tuple[str, str], dict[str, Any]]:
    rows: dict[tuple[str, str], dict[str, Any]] = {}
    for gas in ("co2", "h2o"):
        for idx in range(1, 9):
            analyzer = f"GA{idx:02d}"
            json_path = find_old_ratio_poly_json(run_dir, gas, analyzer)
            if json_path is None:
                continue
            rows[(analyzer, gas.upper())] = old_row_from_json(json_path, analyzer, gas)
    return rows


def load_csv_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def as_float(value: Any) -> float | None:
    if value in (None, "", "None"):
        return None
    try:
        return float(value)
    except Exception:
        return None


def build_comparison(output_dir: Path, run_dir: Path) -> dict[str, Any]:
    old_rows = load_old_rows(run_dir)
    new_rows = load_csv_rows(output_dir / "simplified_coefficients_no_500.csv")
    fit_summary_rows = load_csv_rows(output_dir / "fit_summary_no_500.csv")
    fit_summary_index = {}
    for row in fit_summary_rows:
        analyzer = str(row.get("???") or row.get("Analyzer") or "").strip().upper()
        gas = str(row.get("??") or row.get("Gas") or "").strip().upper()
        if analyzer and gas:
            fit_summary_index[(analyzer, gas)] = row
    comparison_rows: list[dict[str, Any]] = []
    delta_summary: list[str] = []
    for row in new_rows:
        analyzer = str(row.get("???") or row.get("Analyzer") or "").strip().upper()
        gas = str(row.get("??") or row.get("Gas") or "").strip().upper()
        key = (analyzer, gas)
        old = old_rows.get(key, {})
        summary_row = fit_summary_index.get(key, {})
        payload: dict[str, Any] = {
            "Analyzer": analyzer,
            "Gas": gas,
            "old_model_features": old.get("old_model_features", ""),
            "new_model_features": "7feat_noP",
            "new_model_feature_tokens": str(summary_row.get("??????") or "intercept,R,R2,R3,T,T2,RT"),
            "model_feature_policy": str(summary_row.get("??????") or "explicit_config"),
            "a7_a8_handling": "filled_as_0.0_by_offline_chain",
            "old_source": old.get("old_source", ""),
        }
        per_row_delta = []
        for idx in range(9):
            old_value = as_float(old.get(f"old_a{idx}"))
            new_value = as_float(row.get(f"a{idx}"))
            if idx in (7, 8) and new_value is None:
                new_value = 0.0
            payload[f"old_a{idx}"] = old_value
            payload[f"new_a{idx}"] = new_value
            payload[f"delta_a{idx}"] = None if old_value is None or new_value is None else new_value - old_value
            if payload[f"delta_a{idx}"] not in (None, 0.0):
                per_row_delta.append(f"a{idx}={payload[f'delta_a{idx}']:.6g}")
        comparison_rows.append(payload)
        delta_summary.append(f"- {analyzer} {gas}: " + (", ".join(per_row_delta[:8]) if per_row_delta else "no comparable old coefficients or zero delta"))
    if comparison_rows:
        with (output_dir / "comparison.csv").open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(comparison_rows[0].keys()))
            writer.writeheader()
            writer.writerows(comparison_rows)
    md_lines = [
        "# comparison",
        "",
        f"- run_dir: {run_dir}",
        "- new_model_features: intercept,R,R2,R3,T,T2,RT",
        "- note: a7 / a8 are treated as 0.0 by the offline chain when the 7-feature fit does not emit them.",
        "",
        "## delta summary",
    ]
    md_lines.extend(delta_summary or ["- no rows"])
    (output_dir / "comparison.md").write_text("\n".join(md_lines) + "\n", encoding="utf-8")
    return {
        "row_count": len(comparison_rows),
        "comparison_csv": str(output_dir / "comparison.csv"),
        "comparison_md": str(output_dir / "comparison.md"),
        "delta_summary": delta_summary,
    }


def write_model_feature_note(output_dir: Path, run_dir: Path, temp_cfg_path: Path, pressure_row_source: str) -> None:
    lines = [
        "# model features used",
        "",
        f"- run_dir: {run_dir}",
        f"- temp_config: {temp_cfg_path}",
        f"- pressure_row_source: {pressure_row_source}",
        "- forced_model_features: intercept,R,R2,R3,T,T2,RT",
        "- policy: explicit_config (forced in temporary recompute snapshot)",
        "- note: pressure offset remains on the independent pressure row chain and is not merged into CO2/H2O concentration fitting.",
        "- note: a7 / a8 are filled as 0.0 by the existing offline download-plan mapping when the 7-feature fit only yields a0..a6.",
    ]
    (output_dir / "model_features_used.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    write_json(
        output_dir / "model_features_used.json",
        {
            "run_dir": str(run_dir),
            "temp_config": str(temp_cfg_path),
            "pressure_row_source": pressure_row_source,
            "forced_model_features": FORCED_MODEL_FEATURES,
            "policy": "explicit_config",
            "a7_a8_handling": "filled_as_0.0_by_existing_offline_chain",
        },
    )


def choose_pressure_row_source(run_dir: Path) -> str:
    startup_dirs = [path for path in run_dir.glob("startup_pressure_sensor_calibration_*") if path.is_dir()]
    return "startup_calibration" if startup_dirs else "current_ambient"


def process_run(run_dir: Path, classification: dict[str, Any]) -> dict[str, Any]:
    output_dir = run_dir / f"recomputed_ambient_only_7feat_{RUN_STAMP}"
    output_dir.mkdir(parents=True, exist_ok=True)
    staging_dir, temp_cfg_path = build_staging_run(run_dir, output_dir, classification)
    pressure_row_source = choose_pressure_row_source(run_dir)
    result = autodelivery.run_from_cli(
        run_dir=str(staging_dir),
        config_path=str(temp_cfg_path),
        output_dir=str(output_dir),
        write_devices=False,
        verify_report=False,
        pressure_row_source=pressure_row_source,
        verify_short_run_cfg=None,
    )
    write_model_feature_note(output_dir, run_dir, temp_cfg_path, pressure_row_source)
    comparison = build_comparison(output_dir, run_dir)
    fit_summary_rows = load_csv_rows(output_dir / "fit_summary_no_500.csv")
    model_features_by_analyzer_gas = {}
    for row in fit_summary_rows:
        analyzer = str(row.get("???") or row.get("Analyzer") or "").strip().upper()
        gas = str(row.get("??") or row.get("Gas") or "").strip().upper()
        if analyzer and gas:
            model_features_by_analyzer_gas[f"{analyzer}:{gas}"] = str(row.get("??????") or "")
    return {
        "status": "processed",
        "run_dir": str(run_dir),
        "ambient_only": True,
        "ambient_reason": classification.get("ambient_reason", ""),
        "selected_pressure_points": classification.get("selected_pressure_points", []),
        "forced_model_features": FORCED_MODEL_FEATURES,
        "pressure_row_source": pressure_row_source,
        "model_features_by_analyzer_gas": model_features_by_analyzer_gas,
        "output_dir": str(output_dir),
        "temp_config": str(temp_cfg_path),
        "report_path": str(output_dir / "calibration_coefficients.xlsx"),
        "simplified_coefficients": str(output_dir / "simplified_coefficients_no_500.csv"),
        "download_plan": str(output_dir / "download_plan_no_500.csv"),
        "pressure_rows": str(output_dir / "pressure_offset_current_ambient_summary.csv"),
        "comparison": comparison,
        "autodelivery_summary": str(output_dir / "autodelivery_summary.json"),
    }


def main() -> None:
    scanned = []
    processed = []
    skipped = []
    failed = []
    for snapshot in sorted(ROOT.rglob("runtime_config_snapshot.json")):
        run_dir = snapshot.parent.resolve()
        if any(part.startswith("recomputed_ambient_only_7feat_") for part in run_dir.parts):
            continue
        if any(part.startswith("offline_recompute_ambient_only_7feat_") for part in run_dir.parts):
            continue
        scanned.append(str(run_dir))
        classification = classify_run(run_dir)
        classification["run_dir"] = str(run_dir)
        if classification["status"] != "ready":
            skipped.append(classification)
            continue
        try:
            processed.append(process_run(run_dir, classification))
        except Exception as exc:
            failed.append(
                {
                    "status": "failed",
                    "run_dir": str(run_dir),
                    "ambient_only": classification.get("ambient_only", False),
                    "ambient_reason": classification.get("ambient_reason", ""),
                    "selected_pressure_points": classification.get("selected_pressure_points", []),
                    "reason": f"recompute_failed:{exc}",
                }
            )
    payload = {
        "summary_root": str(SUMMARY_ROOT),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "git": {
            "branch": run_git("branch", "--show-current"),
            "head": run_git("rev-parse", "HEAD"),
        },
        "scanned_run_count": len(scanned),
        "processed_count": len(processed),
        "skipped_count": len(skipped),
        "failed_count": len(failed),
        "forced_model_features": FORCED_MODEL_FEATURES,
        "processed_runs": processed,
        "skipped_runs": skipped,
        "failed_runs": failed,
    }
    write_json(SUMMARY_ROOT / "recompute_summary.json", payload)
    lines = [
        "# ambient-only offline recompute summary",
        "",
        f"- git branch: {payload['git']['branch']}",
        f"- git head: {payload['git']['head']}",
        f"- scanned runs: {payload['scanned_run_count']}",
        f"- processed: {payload['processed_count']}",
        f"- skipped: {payload['skipped_count']}",
        f"- failed: {payload['failed_count']}",
        "- forced model_features: intercept,R,R2,R3,T,T2,RT",
        "",
        "## processed runs",
    ]
    lines.extend([f"- {item['run_dir']} -> {item['output_dir']}" for item in processed] or ["- none"])
    lines.extend(["", "## skipped runs"])
    lines.extend([f"- {item['run_dir']}: {item['reason']}" for item in skipped] or ["- none"])
    lines.extend(["", "## failed runs"])
    lines.extend([f"- {item['run_dir']}: {item['reason']}" for item in failed] or ["- none"])
    (SUMMARY_ROOT / "recompute_summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({
        "summary_root": str(SUMMARY_ROOT),
        "processed_count": len(processed),
        "skipped_count": len(skipped),
        "failed_count": len(failed),
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
