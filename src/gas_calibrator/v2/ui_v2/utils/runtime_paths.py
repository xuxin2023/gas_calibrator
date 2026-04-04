from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path


@dataclass(frozen=True)
class RuntimePaths:
    base_dir: Path
    config_dir: Path
    cache_dir: Path
    logs_dir: Path
    screenshots_dir: Path
    plan_profiles_dir: Path
    preferences_path: Path
    recent_runs_path: Path
    route_memory_path: Path

    @classmethod
    def from_base_dir(cls, base_dir: Path) -> "RuntimePaths":
        config_dir = base_dir / "config"
        cache_dir = base_dir / "cache"
        logs_dir = base_dir / "logs"
        screenshots_dir = base_dir / "screenshots"
        return cls(
            base_dir=base_dir,
            config_dir=config_dir,
            cache_dir=cache_dir,
            logs_dir=logs_dir,
            screenshots_dir=screenshots_dir,
            plan_profiles_dir=config_dir / "plan_profiles",
            preferences_path=config_dir / "preferences.json",
            recent_runs_path=config_dir / "recent_runs.json",
            route_memory_path=config_dir / "route_memory.json",
        )

    @classmethod
    def default(cls, app_dir_name: str = "GasCalibratorV2") -> "RuntimePaths":
        local_root = os.environ.get("LOCALAPPDATA")
        base = Path(local_root) if local_root else (Path.home() / ".gas_calibrator_v2")
        return cls.from_base_dir(base / app_dir_name)

    def ensure_dirs(self) -> "RuntimePaths":
        for path in (
            self.base_dir,
            self.config_dir,
            self.cache_dir,
            self.logs_dir,
            self.screenshots_dir,
            self.plan_profiles_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)
        return self
