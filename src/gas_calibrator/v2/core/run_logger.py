"""Run-level CSV logging with schema-resilient headers."""

from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from .csv_resilience import load_csv_rows, merge_csv_headers, save_csv_atomic
from .models import CalibrationPoint


DEFAULT_POINT_COLUMNS = [
    "timestamp",
    "point_index",
    "point_tag",
    "temperature_c",
    "co2_ppm",
    "co2_group",
    "cylinder_nominal_ppm",
    "humidity_pct",
    "pressure_hpa",
    "route",
    "status",
    "stability_time_s",
    "total_time_s",
]
DEFAULT_IO_COLUMNS = ["timestamp", "device", "direction", "data"]


class _CompatibilityHandle:
    """Minimal close-tracked placeholder for legacy tests/callers."""

    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


class RunLogger:
    """Write run-side sample/point/io logs without freezing CSV schema."""

    def __init__(self, output_dir: str, run_id: str):
        self.output_dir = Path(output_dir)
        self.run_id = str(run_id)
        self.run_dir = self.output_dir / self.run_id
        self.run_dir.mkdir(parents=True, exist_ok=True)

        self.samples_path = self.run_dir / "samples_runtime.csv"
        self.points_path = self.run_dir / "points.csv"
        self.io_log_path = self.run_dir / "io_log.csv"

        self._samples_header, self._samples_rows = load_csv_rows(self.samples_path)
        self._points_header, self._points_rows = load_csv_rows(self.points_path)
        self._io_header, self._io_rows = load_csv_rows(self.io_log_path)

        self._samples_header = merge_csv_headers(self._samples_header)
        self._points_header = merge_csv_headers(DEFAULT_POINT_COLUMNS, self._points_header)
        self._io_header = merge_csv_headers(DEFAULT_IO_COLUMNS, self._io_header)

        # Keep compatibility handles for tests/callers that inspect close/finalize
        # behavior. Actual writes go through atomic rewrites, so these placeholders
        # must not hold the target files open on Windows.
        self._samples_writer: Optional[csv.DictWriter] = None

        self._sync_points_file()
        self._sync_io_file()
        if self._samples_rows or self._samples_header:
            self._sync_samples_file()

        self._samples_file = _CompatibilityHandle()
        self._points_file = _CompatibilityHandle()
        self._io_file = _CompatibilityHandle()

    def log_sample(self, row: dict[str, Any]) -> None:
        """Append one runtime sample row with dynamic header expansion."""
        normalized = {str(key): value for key, value in dict(row).items()}
        self._samples_header = merge_csv_headers(self._samples_header, normalized.keys())
        self._samples_rows.append(normalized)
        self._sync_samples_file()

    def log_point(
        self,
        point: CalibrationPoint,
        status: str,
        *,
        point_tag: str = "",
        stability_time_s: Optional[float] = None,
        total_time_s: Optional[float] = None,
        extra_fields: Optional[dict[str, Any]] = None,
    ) -> None:
        """Append one point status row with dynamic header expansion."""
        payload = {
            "timestamp": self._timestamp(),
            "point_index": point.index,
            "point_tag": str(point_tag or ""),
            "temperature_c": point.temperature_c,
            "co2_ppm": point.co2_ppm,
            "co2_group": point.co2_group,
            "cylinder_nominal_ppm": point.cylinder_nominal_ppm,
            "humidity_pct": point.humidity_pct,
            "pressure_hpa": point.pressure_hpa,
            "route": point.route,
            "status": status,
            "stability_time_s": stability_time_s,
            "total_time_s": total_time_s,
        }
        if extra_fields:
            payload.update({str(key): value for key, value in dict(extra_fields).items()})
        self._points_header = merge_csv_headers(DEFAULT_POINT_COLUMNS, self._points_header, payload.keys())
        self._points_rows.append(payload)
        self._sync_points_file()

    def log_io(self, device: str, direction: str, data: str) -> None:
        """Append one device IO row with schema-resilient writes."""
        payload = {
            "timestamp": self._timestamp(),
            "device": str(device),
            "direction": str(direction),
            "data": str(data),
        }
        self._io_header = merge_csv_headers(DEFAULT_IO_COLUMNS, self._io_header, payload.keys())
        self._io_rows.append(payload)
        self._sync_io_file()

    def finalize(self) -> None:
        """Close compatibility file handles after flushing the current state."""
        self._sync_all()
        try:
            self._samples_file.close()
        finally:
            try:
                self._points_file.close()
            finally:
                self._io_file.close()

    close = finalize

    def _sync_all(self) -> None:
        if self._samples_rows or self._samples_header:
            self._sync_samples_file()
        self._sync_points_file()
        self._sync_io_file()

    def _sync_samples_file(self) -> None:
        save_csv_atomic(self.samples_path, self._samples_header, self._samples_rows)

    def _sync_points_file(self) -> None:
        save_csv_atomic(self.points_path, self._points_header, self._points_rows)

    def _sync_io_file(self) -> None:
        save_csv_atomic(self.io_log_path, self._io_header, self._io_rows)

    @staticmethod
    def _timestamp() -> str:
        return datetime.now().isoformat(timespec="milliseconds")
