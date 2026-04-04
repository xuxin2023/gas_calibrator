from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

from ..exporters import export_csv as _export_csv
from ..exporters import export_json as _export_json


def export_json(path: str | Path, payload: Any) -> Path:
    return _export_json(path, payload)


def export_csv(path: str | Path, rows: Iterable[dict[str, Any]]) -> Path:
    return _export_csv(path, rows)
