from __future__ import annotations

import csv
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple


def merge_csv_headers(*groups: Iterable[Any]) -> list[str]:
    merged: list[str] = []
    for group in groups:
        for item in group:
            text = str(item)
            if text and text not in merged:
                merged.append(text)
    return merged


def load_csv_rows(path: Path) -> Tuple[list[str], list[Dict[str, Any]]]:
    file_path = Path(path)
    if not file_path.exists() or file_path.stat().st_size == 0:
        return [], []
    with file_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        header = [str(name) for name in list(reader.fieldnames or []) if str(name)]
        rows = [dict(row) for row in reader]
    return header, rows


def save_csv_atomic(path: Path, fieldnames: Sequence[str], rows: Sequence[Dict[str, Any]]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{target.stem}_",
        suffix=target.suffix,
        dir=str(target.parent),
    )
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        with tmp_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=list(fieldnames),
                extrasaction="ignore",
            )
            if fieldnames:
                writer.writeheader()
            for row in rows:
                writer.writerow(dict(row))
            handle.flush()
        os.replace(tmp_path, target)
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass
