"""CSV artifact row normalization helpers for sidecar validation tools."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, Mapping

from ..logging_utils import _FIELD_LABELS
from .common import latest_artifact, load_csv_rows


_LABEL_TO_KEY = {str(value): str(key) for key, value in _FIELD_LABELS.items()}


def normalize_sample_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    """Map translated CSV headers back to stable internal field keys where possible."""

    normalized: Dict[str, Any] = {}
    for raw_key, value in row.items():
        key = str(raw_key or "")
        mapped = _LABEL_TO_KEY.get(key, key)

        analyzer_match = re.match(r"^气体分析仪(\d+)_(.+)$", key)
        if analyzer_match is None:
            analyzer_match = re.match(r"^姘斾綋鍒嗘瀽浠?(\d+)_(.+)$", key)
        if mapped == key and analyzer_match:
            suffix_text = analyzer_match.group(2)
            suffix_key = _LABEL_TO_KEY.get(suffix_text, suffix_text)
            if suffix_key:
                mapped = f"ga{int(analyzer_match.group(1)):02d}_{suffix_key}"

        if mapped not in normalized or normalized[mapped] in (None, ""):
            normalized[mapped] = value
    return normalized


def load_latest_sample_rows(run_dir: str | Path) -> tuple[Path, list[Dict[str, Any]]]:
    root = Path(run_dir)
    machine_path = root / "samples_machine_readable.csv"
    samples_path = machine_path if machine_path.is_file() else latest_artifact(root, "samples_*.csv")
    if samples_path is None:
        raise FileNotFoundError(f"No samples_*.csv found under {root}")
    rows = [normalize_sample_row(row) for row in load_csv_rows(samples_path)]
    return samples_path, rows
