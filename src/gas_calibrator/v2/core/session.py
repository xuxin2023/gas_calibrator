from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Set

from ..config import AppConfig
from .models import CalibrationPhase, CalibrationPoint


class RunSession:
    """Runtime context for a single calibration run."""

    def __init__(self, config: AppConfig):
        self.run_id: str = datetime.now().strftime("run_%Y%m%d_%H%M%S")
        self.config: AppConfig = config
        self.started_at: Optional[datetime] = None
        self.ended_at: Optional[datetime] = None
        self.phase: CalibrationPhase = CalibrationPhase.IDLE
        self.current_point: Optional[CalibrationPoint] = None
        self.total_points: int = 0
        self.completed_points: int = 0
        self.progress: float = 0.0
        self.enabled_devices: Set[str] = self._collect_enabled_devices(config)
        self.output_dir: Path = Path(config.paths.output_dir) / self.run_id
        self.stop_reason: str = ""
        self.warnings: list[str] = []
        self.errors: list[str] = []

    def start(self) -> None:
        self.started_at = datetime.now()
        self.ended_at = None
        self.stop_reason = ""
        self.phase = CalibrationPhase.IDLE
        self.current_point = None
        self.completed_points = 0
        self.progress = 0.0

    def end(self, reason: str = "") -> None:
        self.ended_at = datetime.now()
        self.stop_reason = str(reason or "")

    def add_warning(self, msg: str) -> None:
        text = str(msg or "").strip()
        if text:
            self.warnings.append(text)

    def add_error(self, msg: str) -> None:
        text = str(msg or "").strip()
        if text:
            self.errors.append(text)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "config": self._serialize_config(self.config),
            "started_at": None if self.started_at is None else self.started_at.isoformat(timespec="seconds"),
            "ended_at": None if self.ended_at is None else self.ended_at.isoformat(timespec="seconds"),
            "phase": self.phase.value,
            "current_point": self._serialize_point(self.current_point),
            "total_points": self.total_points,
            "completed_points": self.completed_points,
            "progress": self.progress,
            "enabled_devices": sorted(self.enabled_devices),
            "output_dir": str(self.output_dir),
            "stop_reason": self.stop_reason,
            "warnings": list(self.warnings),
            "errors": list(self.errors),
        }

    @staticmethod
    def _serialize_config(config: AppConfig) -> Any:
        if is_dataclass(config):
            return asdict(config)
        return config

    @staticmethod
    def _serialize_point(point: Optional[CalibrationPoint]) -> Optional[dict[str, Any]]:
        if point is None:
            return None
        if is_dataclass(point):
            return asdict(point)
        return {
            "index": getattr(point, "index", None),
            "temperature_c": getattr(point, "temperature_c", None),
            "co2_ppm": getattr(point, "co2_ppm", None),
            "humidity_pct": getattr(point, "humidity_pct", None),
            "pressure_hpa": getattr(point, "pressure_hpa", None),
            "route": getattr(point, "route", None),
        }

    @staticmethod
    def _collect_enabled_devices(config: AppConfig) -> Set[str]:
        enabled: set[str] = set()
        devices = config.devices
        single_names = (
            "pressure_controller",
            "pressure_meter",
            "dewpoint_meter",
            "humidity_generator",
            "temperature_chamber",
            "relay_a",
            "relay_b",
        )
        for name in single_names:
            item = getattr(devices, name, None)
            if item is not None and bool(getattr(item, "enabled", True)):
                enabled.add(name)

        for index, item in enumerate(getattr(devices, "gas_analyzers", []) or []):
            if bool(getattr(item, "enabled", True)):
                enabled.add(f"gas_analyzer_{index}")
        return enabled
