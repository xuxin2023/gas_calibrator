from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


def export_json(path: str | Path, payload: Any) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return output_path


def export_csv(path: str | Path, rows: Iterable[dict[str, Any]]) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    materialized = [dict(row) for row in rows]
    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in materialized:
        for key in row.keys():
            key_text = str(key)
            if key_text in seen:
                continue
            seen.add(key_text)
            fieldnames.append(key_text)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        if not fieldnames:
            handle.write("")
            return output_path
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in materialized:
            writer.writerow(row)
    return output_path
