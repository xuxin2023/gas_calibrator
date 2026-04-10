"""Markdown/HTML/Excel report helpers."""

from __future__ import annotations

import html
from pathlib import Path
from typing import Mapping

import pandas as pd


def _table_to_markdown(frame: pd.DataFrame, max_rows: int = 20) -> str:
    if frame.empty:
        return "_No rows._"
    clipped = frame.head(max_rows).copy()
    columns = [str(column) for column in clipped.columns]
    header = "| " + " | ".join(columns) + " |"
    sep = "| " + " | ".join("---" for _ in columns) + " |"
    rows = []
    for values in clipped.astype(object).where(pd.notna(clipped), "").itertuples(index=False, name=None):
        rows.append("| " + " | ".join(str(value) for value in values) + " |")
    return "\n".join([header, sep, *rows])


def render_report_markdown(report: Mapping[str, object]) -> str:
    """Render a human-readable markdown report."""

    lines: list[str] = []
    lines.append(f"# Offline Absorbance Debug Report: {report['run_name']}")
    lines.append("")
    lines.append("## 1. Data inventory")
    lines.append(f"- Input source: `{report['input_path']}`")
    lines.append(f"- Output directory: `{report['output_dir']}`")
    lines.append(f"- Total points identified: `{report['point_count']}`")
    lines.append(f"- CO2 points identified: `{report['co2_point_count']}`")
    lines.append(f"- H2O points identified: `{report['h2o_point_count']}`")
    lines.append("")
    lines.append("## 2. Available analyzers")
    lines.append(f"- Main analyzers: `{', '.join(report['main_analyzers'])}`")
    lines.append(f"- Warning-only analyzers: `{', '.join(report['warning_only_analyzers'])}`")
    lines.append(f"- Analyzers seen in data: `{', '.join(report['detected_analyzers'])}`")
    lines.append("")
    lines.append("## 3. Temperature coverage")
    lines.append(f"- CO2 temperatures: `{report['co2_temperatures']}`")
    lines.append(f"- Zero-gas temperatures: `{report['zero_temperatures']}`")
    lines.append(f"- Missing zero-gas temperatures: `{report['missing_zero_temperatures']}`")
    lines.append("- 40 C is excluded from R0(T) fitting because the run has no 0 ppm point there.")
    lines.append("")
    lines.append("## 4. Formula summary")
    for label, formula in report["formulas"].items():
        lines.append(f"- {label}: `{formula}`")
    lines.append("")
    lines.append("## 5. Rule Freeze for Fair Absorbance Challenge")
    for item in report["rule_freeze"]:
        lines.append(f"- {item}")
    lines.append("")
    lines.append("## 6. Validation checks")
    lines.append(_table_to_markdown(report["validation_table"], max_rows=20))
    lines.append("")
    lines.append("## 7. Invalid pressure exclusion")
    lines.append("### Selected matched sources")
    lines.append(_table_to_markdown(report["selected_source_summary"], max_rows=12))
    lines.append("")
    lines.append("### Invalid pressure summary")
    lines.append(_table_to_markdown(report["invalid_pressure_summary"], max_rows=12))
    lines.append("")
    lines.append("### Invalid pressure points")
    lines.append(_table_to_markdown(report["invalid_pressure_points"], max_rows=20))
    lines.append("")
    lines.append("## 8. Key coefficients")
    lines.append("### Temperature correction")
    lines.append(_table_to_markdown(report["temperature_coefficients"], max_rows=12))
    lines.append("")
    lines.append("### Pressure correction")
    lines.append(_table_to_markdown(report["pressure_coefficients"], max_rows=12))
    lines.append("")
    lines.append("### R0(T)")
    lines.append(_table_to_markdown(report["r0_coefficients"], max_rows=18))
    lines.append("")
    lines.append("## 9. Absorbance model selection")
    lines.append("### Selected models")
    lines.append(_table_to_markdown(report["absorbance_model_selection"], max_rows=12))
    lines.append("")
    lines.append("### Candidate scores")
    lines.append(_table_to_markdown(report["absorbance_model_scores"], max_rows=24))
    lines.append("")
    lines.append("## 10. Why New Chain Loses")
    if report["diagnostic_top_lines"]:
        for item in report["diagnostic_top_lines"]:
            lines.append(f"- {item}")
    lines.append(f"- Implementation assessment: `{report['implementation_issue']}`")
    lines.append("")
    lines.append("### Absorbance order compare")
    lines.append(_table_to_markdown(report["order_compare"], max_rows=12))
    lines.append("")
    lines.append("### R0 source consistency")
    lines.append(_table_to_markdown(report["source_consistency"], max_rows=12))
    lines.append("")
    lines.append("### Pressure branch compare")
    lines.append(_table_to_markdown(report["pressure_branch_compare"], max_rows=12))
    lines.append("")
    lines.append("### Upper bound vs deployable")
    lines.append(_table_to_markdown(report["upper_bound_vs_deployable"], max_rows=12))
    lines.append("")
    lines.append("### Root cause ranking")
    lines.append(_table_to_markdown(report["root_cause_ranking"], max_rows=12))
    lines.append("")
    lines.append("## 11. Old vs new comparison")
    lines.append("- Main conclusion uses the valid-only chain after invalid-pressure exclusion.")
    lines.append("- New-chain comparison uses the selected absorbance model with grouped validation predictions when available.")
    lines.append("")
    lines.append("### Overview summary")
    lines.append(_table_to_markdown(report["overview_summary"], max_rows=20))
    lines.append("")
    lines.append("### By temperature")
    lines.append(_table_to_markdown(report["by_temperature"], max_rows=24))
    lines.append("")
    lines.append("### By concentration range")
    lines.append(_table_to_markdown(report["by_concentration_range"], max_rows=24))
    lines.append("")
    lines.append("### Zero-point special")
    lines.append(_table_to_markdown(report["zero_special"], max_rows=24))
    lines.append("")
    lines.append("### Regression overall")
    lines.append(_table_to_markdown(report["regression_overall"], max_rows=24))
    if report["comparison_conclusions"]:
        lines.append("")
        lines.append("### Analyzer conclusions")
        for item in report["comparison_conclusions"]:
            lines.append(f"- {item}")
    lines.append("")
    lines.append("## 12. Before/after tightening")
    lines.append(_table_to_markdown(report["before_after_summary"], max_rows=12))
    lines.append("")
    lines.append("## 13. Full-data appendix")
    lines.append("### Full-data overview summary")
    lines.append(_table_to_markdown(report["appendix_overview_summary"], max_rows=20))
    lines.append("")
    lines.append("### Full-data automatic conclusion")
    lines.append(_table_to_markdown(report["appendix_auto_conclusions"], max_rows=10))
    lines.append("")
    lines.append("## 14. Automatic conclusion page")
    lines.append(_table_to_markdown(report["auto_conclusions"], max_rows=10))
    lines.append("")
    lines.append("## 15. Base/final mode")
    lines.append(f"- Enabled: `{report['base_final_enabled']}`")
    lines.append(f"- Source: `{report['base_final_source']}`")
    lines.append("")
    lines.append("## 16. Limitations")
    for item in report["limitations"]:
        lines.append(f"- {item}")
    lines.append("")
    lines.append("## 17. Suggested next experiments")
    for item in report["next_steps"]:
        lines.append(f"- {item}")
    lines.append("")
    return "\n".join(lines)


def render_report_html(report: Mapping[str, object]) -> str:
    """Render a compact standalone HTML report."""

    md = render_report_markdown(report)
    paragraphs = []
    for block in md.split("\n\n"):
        escaped = html.escape(block)
        if block.startswith("#"):
            level = len(block.split(" ", 1)[0])
            text = html.escape(block[level + 1 :])
            paragraphs.append(f"<h{level}>{text}</h{level}>")
        else:
            paragraphs.append(f"<pre>{escaped}</pre>")
    body = "\n".join(paragraphs)
    return (
        "<!doctype html><html><head><meta charset='utf-8'>"
        "<title>Offline Absorbance Debug Report</title>"
        "<style>body{font-family:Segoe UI,Arial,sans-serif;margin:24px;line-height:1.45;}"
        "pre{white-space:pre-wrap;background:#f7f7f7;padding:12px;border-radius:8px;}"
        "h1,h2,h3{color:#14324a;} table{border-collapse:collapse;} </style>"
        "</head><body>"
        f"{body}"
        "</body></html>"
    )


def write_workbook(path: Path, sheets: Mapping[str, pd.DataFrame]) -> None:
    """Write multiple dataframes into one Excel workbook."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, frame in sheets.items():
            safe_name = str(name)[:31] or "Sheet1"
            frame.to_excel(writer, sheet_name=safe_name, index=False)
