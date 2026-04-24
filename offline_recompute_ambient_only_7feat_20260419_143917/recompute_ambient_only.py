from __future__ import annotations

import csv
import json
import subprocess
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from gas_calibrator.tools import run_v1_corrected_autodelivery as autodelivery

ROOT = Path(r"D:/gas_calibrator")
SUMMARY_ROOT = Path(r"D:/gas_calibrator/offline_recompute_ambient_only_7feat_20260419_143917")
FORCED_MODEL_FEATURES = ["intercept", "R", "R2", "R3", "T", "T2", "RT"]
RECOMPUTE_STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")


def run_git(*args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True, encoding="utf-8").strip()


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def latest_matching(run_dir: Path, pattern: str) -> Path | None:
    matches = [path for path in run_dir.glob(pattern) if path.is_file()]
    return max(matches, key=lambda item: item.stat().st_mtime) if matches else None


def resolve_summary_paths(run_dir: Path) -> tuple[Path | None, Path | None]:
    gas = latest_matching(run_dir, "?????_??_*.xlsx") or latest_matching(run_dir, "?????_??_*.csv")
    water = latest_matching(run_dir, "?????_??_*.xlsx") or latest_matching(run_dir, "?????_??_*.csv")
    return gas, water


def resolve_samples_path(run_dir: Path) -> Path | None:
    path = latest_matching(run_dir, "samples_*.csv")
    if path is not None:
        return path
    fallback = run_dir / "samples.csv"
    return fallback if fallback.exists() else None


def normalize_selected_pressure_points(raw: Any) -> list[str]:
    if raw in (None, "", []):
        return []
    values = raw if isinstance(raw, list) else [raw]
    return [str(item).strip().lower() for item in values if str(item).strip()]


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
    gas_summary, water_summary = resolve_summary_paths(run_dir)
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
                "gas_summary": str(gas_summary) if gas_summary else "",
                "water_summary": str(water_summary) if water_summary else "",
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
                "gas_summary": str(gas_summary) if gas_summary else "",
                "water_summary": str(water_summary) if water_summary else "",
                "samples_path": str(samples_path) if samples_path else "",
            }
    missing = []
    if gas_summary is None:
        missing.append("gas_summary")
    if water_summary is None:
        missing.append("water_summary")
    if samples_path is None:
        missing.append("samples_csv")
    if missing:
        return {
            "status": "skipped",
            "reason": f"missing_required_files:{','.join(missing)}",
            "ambient_only": ambient_only,
            "ambient_reason": ambient_reason,
            "selected_pressure_points": selected,
            "gas_summary": str(gas_summary) if gas_summary else "",
            "water_summary": str(water_summary) if water_summary else "",
            "samples_path": str(samples_path) if samples_path else "",
        }
    return {
        "status": "ready",
        "ambient_only": True,
        "ambient_reason": ambient_reason,
        "selected_pressure_points": selected,
        "gas_summary": str(gas_summary),
        "water_summary": str(water_summary),
        "samples_path": str(samples_path),
        "cfg": cfg,
    }


def find_old_ratio_poly_json(run_dir: Path, gas: str, analyzer: str) -> Path | None:
    pattern = f"{gas.lower()}_{analyzer.upper()}_ratio_poly_fit_*.json"
    return latest_matching(run_dir, pattern)


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
    conclusion_lines: list[str] = []
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
            "a7_a8_handling": "filled_as_0.0_by_offline_chain" if (row.get("a7") in (None, "") or row.get("a8") in (None, "")) else "reported_in_new_output",
            "old_source": old.get("old_source", ""),
        }
        delta_nonzero = []
        for idx in range(9):
            old_value = as_float(old.get(f"old_a{idx}"))
            new_raw = row.get(f"a{idx}")
            new_value = as_float(new_raw)
            if idx in (7, 8) and new_value is None:
                new_value = 0.0
            payload[f"old_a{idx}"] = old_value
            payload[f"new_a{idx}"] = new_value
            payload[f"delta_a{idx}"] = None if old_value is None or new_value is None else new_value - old_value
            if payload[f"delta_a{idx}"] not in (None, 0.0):
                delta_nonzero.append(f"a{idx}={payload[f'delta_a{idx}']:.6g}")
        comparison_rows.append(payload)
        if delta_nonzero:
            conclusion_lines.append(f"- {analyzer} {gas}: " + ", ".join(delta_nonzero[:6]))
        else:
            conclusion_lines.append(f"- {analyzer} {gas}: no comparable old coefficients found or no delta")
    if comparison_rows:
        fieldnames = list(comparison_rows[0].keys())
        with (output_dir / "comparison.csv").open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(comparison_rows)
    md_lines = [
        "# comparison",
        "",
        f"- run_dir: {run_dir}",
        "- new_model_features: intercept,R,R2,R3,T,T2,RT",
        "- note: a7 / a8 are treated as 0.0 by the offline download-plan chain when the 7-feature fit does not emit them.",
        "",
        "## delta summary",
    ]
    md_lines.extend(conclusion_lines or ["- no rows"])
    (output_dir / "comparison.md").write_text("\n".join(md_lines) + "\n", encoding="utf-8")
    return {
        "row_count": len(comparison_rows),
        "rows": comparison_rows,
        "delta_summary": conclusion_lines,
        "comparison_csv": str(output_dir / "comparison.csv"),
        "comparison_md": str(output_dir / "comparison.md"),
    }


def write_model_feature_note(output_dir: Path, run_dir: Path, temp_cfg_path: Path) -> None:
    lines = [
        "# model features used",
        "",
        f"- run_dir: {run_dir}",
        f"- temp_config: {temp_cfg_path}",
        "- forced_model_features: intercept,R,R2,R3,T,T2,RT",
        "- policy: explicit_config (forced in temporary recompute snapshot)",
        "- note: pressure offset remains on the independent SENCO9/current-ambient pressure row chain and is not merged back into CO2/H2O concentration fitting.",
        "- note: a7 / a8 are filled as 0.0 by the existing offline download-plan mapping when the 7-feature fit only yields a0..a6.",
    ]
    (output_dir / "model_features_used.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    write_json(
        output_dir / "model_features_used.json",
        {
            "run_dir": str(run_dir),
            "temp_config": str(temp_cfg_path),
            "forced_model_features": FORCED_MODEL_FEATURES,
            "policy": "explicit_config",
            "pressure_offset_chain": "independent",
            "a7_a8_handling": "filled_as_0.0_by_existing_offline_chain",
        },
    )


def process_run(run_dir: Path, classification: dict[str, Any]) -> dict[str, Any]:
    cfg = dict(classification["cfg"])
    coeff_cfg = dict(cfg.get("coefficients") or {})
    coeff_cfg["model_features"] = list(FORCED_MODEL_FEATURES)
    cfg["coefficients"] = coeff_cfg
    if not classification.get("selected_pressure_points"):
        workflow_cfg = dict(cfg.get("workflow") or {})
        workflow_cfg["selected_pressure_points"] = ["ambient"]
        cfg["workflow"] = workflow_cfg
    output_dir = run_dir / f"recomputed_ambient_only_7feat_{RECOMPUTE_STAMP}"
    output_dir.mkdir(parents=True, exist_ok=True)
    temp_cfg_path = output_dir / "runtime_config_snapshot_recompute.json"
    write_json(temp_cfg_path, cfg)
    result = autodelivery.run_from_cli(
        run_dir=str(run_dir),
        config_path=str(temp_cfg_path),
        output_dir=str(output_dir),
        write_devices=False,
        verify_report=False,
        verify_short_run_cfg=None,
    )
    write_model_feature_note(output_dir, run_dir, temp_cfg_path)
    comparison = build_comparison(output_dir, run_dir)
    fit_summary_rows = load_csv_rows(output_dir / "fit_summary_no_500.csv")
    model_features_by_gas = {}
    for row in fit_summary_rows:
        analyzer = str(row.get("???") or "").strip().upper()
        gas = str(row.get("??") or "").strip().upper()
        if analyzer and gas:
            model_features_by_gas[f"{analyzer}:{gas}"] = str(row.get("??????") or "")
    return {
        "status": "processed",
        "run_dir": str(run_dir),
        "ambient_only": True,
        "ambient_reason": classification.get("ambient_reason", ""),
        "selected_pressure_points": classification.get("selected_pressure_points", []),
        "forced_model_features": FORCED_MODEL_FEATURES,
        "model_features_by_analyzer_gas": model_features_by_gas,
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
    if processed:
        for item in processed:
            lines.append(f"- {item['run_dir']} -> {item['output_dir']}")
            lines.append(f"  ambient_reason: {item['ambient_reason']}")
            lines.append(f"  report_path: {item['report_path']}")
            for delta in item['comparison']['delta_summary'][:12]:
                lines.append(f"  {delta}")
    else:
        lines.append("- none")
    lines.extend(["", "## skipped runs"])
    if skipped:
        for item in skipped:
            lines.append(f"- {item['run_dir']}: {item['reason']}")
    else:
        lines.append("- none")
    lines.extend(["", "## failed runs"])
    if failed:
        for item in failed:
            lines.append(f"- {item['run_dir']}: {item['reason']}")
    else:
        lines.append("- none")
    (SUMMARY_ROOT / "recompute_summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({
        'summary_root': str(SUMMARY_ROOT),
        'processed_count': len(processed),
        'skipped_count': len(skipped),
        'failed_count': len(failed),
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
