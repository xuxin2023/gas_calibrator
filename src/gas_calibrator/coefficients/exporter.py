"""建模结果导出工具。"""

from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pandas as pd

from .prediction_analysis import PredictionAnalysisResult


def export_model_comparison(
    payload: Dict[str, Any],
    comparison_rows: Iterable[Dict[str, Any]],
    out_dir: Path,
    *,
    prefix: str,
) -> Dict[str, Path]:
    """导出模型比较结果为 JSON 和 CSV。"""
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = out_dir / f"{prefix}_model_compare_{stamp}.json"
    csv_path = out_dir / f"{prefix}_model_compare_{stamp}.csv"

    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    rows = list(comparison_rows)
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        if rows:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
        else:
            handle.write("")
    return {"json": json_path, "csv": csv_path}


def export_prediction_analysis(
    analysis: PredictionAnalysisResult,
    out_dir: Path,
    *,
    prefix: str,
) -> Dict[str, Path]:
    """导出逐点回代分析结果为 CSV 和 Excel。"""
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    points_csv = out_dir / f"{prefix}_prediction_analysis_{stamp}.csv"
    summary_csv = out_dir / f"{prefix}_prediction_analysis_summary_{stamp}.csv"
    range_csv = out_dir / f"{prefix}_prediction_analysis_ranges_{stamp}.csv"
    excel_path = out_dir / f"{prefix}_prediction_analysis_{stamp}.xlsx"

    analysis.point_table.to_csv(points_csv, index=False, encoding="utf-8-sig")
    pd.DataFrame([analysis.summary]).to_csv(summary_csv, index=False, encoding="utf-8-sig")
    analysis.range_table.to_csv(range_csv, index=False, encoding="utf-8-sig")

    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        analysis.point_table.to_excel(writer, sheet_name="逐点对比", index=False)
        pd.DataFrame([analysis.summary]).to_excel(writer, sheet_name="统计摘要", index=False)
        analysis.range_table.to_excel(writer, sheet_name="分区间分析", index=False)
        analysis.top_error_orig.to_excel(writer, sheet_name="原始误差TopN", index=False)
        analysis.top_error_simple.to_excel(writer, sheet_name="简化误差TopN", index=False)
        analysis.top_pred_diff.to_excel(writer, sheet_name="简化影响TopN", index=False)

    return {
        "points_csv": points_csv,
        "summary_csv": summary_csv,
        "ranges_csv": range_csv,
        "excel": excel_path,
    }
