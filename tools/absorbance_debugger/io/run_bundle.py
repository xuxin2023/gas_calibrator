"""Helpers for reading completed run data from zip files or directories."""

from __future__ import annotations

import json
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

from ..models.config import RunArtifacts


@dataclass(frozen=True)
class RunBundle:
    """Abstract a completed run stored as a directory or zip archive."""

    source_path: Path

    @property
    def is_zip(self) -> bool:
        return self.source_path.is_file() and self.source_path.suffix.lower() == ".zip"

    def list_files(self) -> list[str]:
        """Return all file names relative to the source root."""

        if self.is_zip:
            with zipfile.ZipFile(self.source_path) as archive:
                return [name for name in archive.namelist() if not name.endswith("/")]
        return [
            str(path.relative_to(self.source_path)).replace("\\", "/")
            for path in self.source_path.rglob("*")
            if path.is_file()
        ]

    def read_text(self, relative_path: str, encoding: str = "utf-8-sig") -> str:
        """Read a text file from the bundle."""

        if self.is_zip:
            with zipfile.ZipFile(self.source_path) as archive:
                return archive.read(relative_path).decode(encoding)
        return (self.source_path / Path(relative_path)).read_text(encoding=encoding)

    def read_bytes(self, relative_path: str) -> bytes:
        """Read a binary file from the bundle."""

        if self.is_zip:
            with zipfile.ZipFile(self.source_path) as archive:
                return archive.read(relative_path)
        return (self.source_path / Path(relative_path)).read_bytes()

    def read_csv(self, relative_path: str, **kwargs) -> pd.DataFrame:
        """Read a CSV file into a dataframe."""

        if self.is_zip:
            with zipfile.ZipFile(self.source_path) as archive:
                with archive.open(relative_path) as handle:
                    return pd.read_csv(handle, **kwargs)
        return pd.read_csv(self.source_path / Path(relative_path), **kwargs)

    def read_json(self, relative_path: str) -> dict:
        """Read a JSON document."""

        return json.loads(self.read_text(relative_path, encoding="utf-8"))


def _pick_single(files: Iterable[str], keyword: str) -> str | None:
    matches = sorted(name for name in files if keyword in name)
    if not matches:
        return None
    return matches[0]


def discover_run_artifacts(bundle: RunBundle) -> RunArtifacts:
    """Resolve standard artifact file names from a completed run bundle."""

    files = bundle.list_files()
    if not files:
        raise FileNotFoundError(f"No files found under {bundle.source_path}")

    root_prefix = ""
    first = files[0]
    if "/" in first:
        root_prefix = first.split("/", 1)[0]
    run_name = root_prefix or bundle.source_path.stem

    resolved = {
        "samples": _pick_single(files, "samples_"),
        "points_readable": _pick_single(files, "points_readable_") if _pick_single(files, "points_readable_") and _pick_single(files, "points_readable_").endswith(".csv") else None,
        "points": _pick_single(files, "points_") if _pick_single(files, "points_") and _pick_single(files, "points_").endswith(".csv") else None,
        "pressure_offset_summary": _pick_single(files, "pressure_offset_summary.csv"),
        "runtime_config": _pick_single(files, "runtime_config_snapshot.json"),
        "io": _pick_single(files, "io_"),
    }

    # Prefer CSV over XLSX for points_readable.
    readable_matches = sorted(name for name in files if "points_readable_" in name and name.endswith(".csv"))
    if readable_matches:
        resolved["points_readable"] = readable_matches[0]

    point_matches = sorted(
        name for name in files if "points_" in name and name.endswith(".csv") and "points_readable_" not in name
    )
    if point_matches:
        resolved["points"] = point_matches[0]

    missing = [name for name in ("samples", "points_readable", "points", "runtime_config") if not resolved.get(name)]
    if missing:
        joined = ", ".join(missing)
        raise FileNotFoundError(f"Missing required run artifacts: {joined}")

    return RunArtifacts(run_name=run_name, root_prefix=root_prefix, files=resolved)
