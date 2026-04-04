"""Shared validation report writers.

These helpers are intentionally sidecar-only. They are used by validation tools
that run without changing the V1 production calibration workflow timing.
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font


@dataclass
class ValidationMetadata:
    """Common metadata persisted by sidecar validation tools."""

    tool_name: str
    created_at: str = field(default_factory=lambda: datetime.now().isoformat(timespec="seconds"))
    analyzers: List[str] = field(default_factory=list)
    input_paths: List[str] = field(default_factory=list)
    output_dir: str = ""
    config_path: str = ""
    config_summary: Dict[str, Any] = field(default_factory=dict)
    notes: List[str] = field(default_factory=list)


def _normalize_table_rows(rows: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for row in rows:
        normalized.append(dict(row))
    return normalized


def _table_header(rows: List[Dict[str, Any]]) -> List[str]:
    header: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in header:
                header.append(str(key))
    return header


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    header = _table_header(rows)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _autosize_sheet(ws) -> None:
    for column_cells in ws.columns:
        width = 10
        for cell in column_cells:
            value = "" if cell.value is None else str(cell.value)
            width = max(width, min(60, len(value) + 2))
        ws.column_dimensions[column_cells[0].column_letter].width = width


def write_validation_report(
    output_dir: str | Path,
    *,
    prefix: str,
    metadata: ValidationMetadata,
    tables: Mapping[str, Iterable[Mapping[str, Any]]],
) -> Dict[str, Path]:
    """Write a validation workbook plus per-table CSV artifacts."""

    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)

    workbook_path = root / f"{prefix}.xlsx"
    metadata_path = root / f"{prefix}_meta.json"
    workbook = Workbook()
    meta_ws = workbook.active
    meta_ws.title = "meta"
    meta_ws.append(["field", "value"])
    for key, value in asdict(metadata).items():
        if isinstance(value, (dict, list)):
            rendered = json.dumps(value, ensure_ascii=False, indent=2)
        else:
            rendered = value
        meta_ws.append([key, rendered])
    meta_ws.freeze_panes = "A2"
    for cell in meta_ws[1]:
        cell.font = Font(bold=True)
        cell.alignment = Alignment(vertical="top")
    _autosize_sheet(meta_ws)

    outputs: Dict[str, Path] = {"workbook": workbook_path, "metadata": metadata_path}
    for table_name, raw_rows in tables.items():
        rows = _normalize_table_rows(raw_rows)
        csv_path = root / f"{table_name}.csv"
        _write_csv(csv_path, rows)
        outputs[f"{table_name}_csv"] = csv_path

        ws = workbook.create_sheet(title=str(table_name)[:31] or "sheet")
        header = _table_header(rows)
        if header:
            ws.append(header)
            for row in rows:
                ws.append([row.get(key) for key in header])
            ws.freeze_panes = "A2"
            for cell in ws[1]:
                cell.font = Font(bold=True)
        _autosize_sheet(ws)

    workbook.save(workbook_path)
    metadata_path.write_text(
        json.dumps(asdict(metadata), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    workbook.close()
    return outputs
