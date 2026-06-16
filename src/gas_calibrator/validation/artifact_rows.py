"""CSV artifact row normalization helpers for sidecar validation tools."""

from __future__ import annotations

import os
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
            analyzer_match = re.match(r"^姘斾綋鍒嗘瀽浠(\d+)_(.+)$", key)
        if analyzer_match is None:
            analyzer_match = re.match(r"^濮樻柧缍嬮崚鍡樼€芥禒(\d+)_(.+)$", key)
        if mapped == key and analyzer_match:
            suffix_text = analyzer_match.group(2)
            suffix_key = _LABEL_TO_KEY.get(suffix_text, suffix_text)
            if suffix_key:
                mapped = f"ga{int(analyzer_match.group(1)):02d}_{suffix_key}"

        if mapped not in normalized or normalized[mapped] in (None, ""):
            normalized[mapped] = value
    return normalized


def _sample_file_in(directory: Path) -> Path | None:
    machine_path = directory / "samples_machine_readable.csv"
    if machine_path.is_file():
        return machine_path
    return latest_artifact(directory, "samples_*.csv")


def _root_sample_paths(root: Path) -> list[Path]:
    path = _sample_file_in(root)
    return [path] if path is not None else []


def _open_flow_sample_paths(root: Path) -> list[Path]:
    """Find queue point samples while avoiding reverify and diagnostic folders."""

    paths: list[Path] = []
    for route_name in ("co2_open_flow", "h2o_open_flow"):
        route_dir = root / route_name
        if not route_dir.is_dir():
            continue
        for directory in sorted((item for item in route_dir.rglob("*") if item.is_dir()), key=str):
            name = directory.name.lower()
            if not (name.startswith("p") or "_t" in name or "hg" in name):
                continue
            path = _sample_file_in(directory)
            if path is not None:
                paths.append(path)
    return paths


def _candidate_aggregate_sample_paths(root: Path) -> list[Path]:
    paths: list[Path] = []
    for component in ("co2", "h2o"):
        candidates: list[Path] = []
        for directory in root.glob("candidate_fit*"):
            aggregate_dir = directory / f"{component}_aggregate"
            path = _sample_file_in(aggregate_dir) if aggregate_dir.is_dir() else None
            if path is not None:
                candidates.append(path)
        if candidates:
            paths.append(max(candidates, key=lambda item: item.stat().st_mtime))
    return paths


def _dedupe_paths(paths: list[Path]) -> list[Path]:
    seen: set[Path] = set()
    deduped: list[Path] = []
    for path in paths:
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        deduped.append(path)
    return deduped


def _representative_sample_path(paths: list[Path]) -> Path:
    if len(paths) == 1:
        return paths[0]
    common = os.path.commonpath([str(path.resolve()) for path in paths])
    return Path(common)


def load_latest_sample_rows(run_dir: str | Path) -> tuple[Path, list[Dict[str, Any]]]:
    root = Path(run_dir)
    sample_paths = _root_sample_paths(root)
    if not sample_paths:
        sample_paths = _open_flow_sample_paths(root)
    if not sample_paths:
        sample_paths = _candidate_aggregate_sample_paths(root)
    sample_paths = _dedupe_paths(sample_paths)
    if not sample_paths:
        raise FileNotFoundError(f"No samples_*.csv found under {root}")
    rows: list[Dict[str, Any]] = []
    for path in sample_paths:
        rows.extend(normalize_sample_row(row) for row in load_csv_rows(path))
    return _representative_sample_path(sample_paths), rows
