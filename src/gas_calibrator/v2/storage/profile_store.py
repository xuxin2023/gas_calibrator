from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Optional

from ..domain.plan_models import CalibrationPlanProfile


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_profile_name(name: str) -> str:
    normalized = str(name or "").strip()
    if not normalized:
        raise ValueError("profile name is required")
    return normalized


def _slugify(value: str) -> str:
    text = re.sub(r"[^A-Za-z0-9._-]+", "-", str(value or "").strip()).strip("._-").lower()
    return text or "profile"


@dataclass(frozen=True)
class ProfileSummary:
    name: str
    profile_version: str = "1.0"
    description: str = ""
    is_default: bool = False
    path: str = ""
    updated_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "profile_version": self.profile_version,
            "description": self.description,
            "is_default": self.is_default,
            "path": self.path,
            "updated_at": self.updated_at,
        }


class ProfileStore:
    """File-backed store for editable calibration plan profiles."""

    def __init__(self, base_dir: Path) -> None:
        self.base_dir = Path(base_dir)
        self.profiles_dir = self.base_dir / "profiles"
        self.index_path = self.base_dir / "index.json"

    def list_profiles(self) -> list[ProfileSummary]:
        index = self._load_index()
        default_name = index.get("default_profile_name")
        summaries: list[ProfileSummary] = []
        for name, entry in index.get("profiles", {}).items():
            profile = self.load_profile(name)
            if profile is None:
                continue
            summaries.append(
                ProfileSummary(
                    name=profile.name,
                    profile_version=str(getattr(profile, "profile_version", "1.0") or "1.0"),
                    description=profile.description,
                    is_default=profile.name == default_name,
                    path=str(self._profile_path_from_entry(entry)),
                    updated_at=str(entry.get("updated_at", "")),
                )
            )
        summaries.sort(key=lambda item: (not item.is_default, item.name.lower()))
        return summaries

    def load_profile(self, name: str) -> CalibrationPlanProfile | None:
        index = self._load_index()
        normalized_name = _normalize_profile_name(name)
        entry = index.get("profiles", {}).get(normalized_name)
        if not isinstance(entry, dict):
            return None
        path = self._profile_path_from_entry(entry)
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None
        if not isinstance(payload, dict):
            return None
        profile = CalibrationPlanProfile.from_dict(payload)
        profile.name = normalized_name
        profile.is_default = normalized_name == index.get("default_profile_name")
        return profile

    def save_profile(self, profile: CalibrationPlanProfile) -> CalibrationPlanProfile:
        normalized_name = _normalize_profile_name(profile.name)
        index = self._load_index()
        profiles = dict(index.get("profiles", {}))
        entry = profiles.get(normalized_name)
        filename = str(entry.get("file")) if isinstance(entry, dict) and entry.get("file") else self._build_filename(normalized_name)
        payload = profile.to_dict()
        payload["name"] = normalized_name
        payload["is_default"] = False

        path = self.profiles_dir / filename
        self.profiles_dir.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

        profiles[normalized_name] = {
            "file": filename,
            "updated_at": _utc_now_iso(),
        }
        index["profiles"] = profiles
        if bool(profile.is_default):
            index["default_profile_name"] = normalized_name
        elif index.get("default_profile_name") == normalized_name:
            index["default_profile_name"] = normalized_name
        self._save_index(index)
        loaded = self.load_profile(normalized_name)
        if loaded is None:
            raise RuntimeError(f"failed to reload saved profile: {normalized_name}")
        return loaded

    def delete_profile(self, name: str) -> bool:
        normalized_name = _normalize_profile_name(name)
        index = self._load_index()
        profiles = dict(index.get("profiles", {}))
        entry = profiles.pop(normalized_name, None)
        if not isinstance(entry, dict):
            return False
        path = self._profile_path_from_entry(entry)
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        index["profiles"] = profiles
        if index.get("default_profile_name") == normalized_name:
            index["default_profile_name"] = None
        self._save_index(index)
        return True

    def set_default_profile(self, name: str) -> CalibrationPlanProfile:
        normalized_name = _normalize_profile_name(name)
        index = self._load_index()
        if normalized_name not in index.get("profiles", {}):
            raise ValueError(f"profile not found: {normalized_name}")
        index["default_profile_name"] = normalized_name
        self._save_index(index)
        profile = self.load_profile(normalized_name)
        if profile is None:
            raise RuntimeError(f"failed to reload default profile: {normalized_name}")
        return profile

    def get_default_profile(self) -> CalibrationPlanProfile | None:
        index = self._load_index()
        default_name = index.get("default_profile_name")
        if not default_name:
            return None
        return self.load_profile(str(default_name))

    def export_profile(self, name: str, destination: Path) -> Path:
        profile = self.load_profile(name)
        if profile is None:
            raise ValueError(f"profile not found: {name}")
        path = Path(destination)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(profile.to_dict(), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return path

    def import_profile(
        self,
        source: Path,
        *,
        set_default: Optional[bool] = None,
    ) -> CalibrationPlanProfile:
        path = Path(source)
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError(f"invalid profile payload: {path}")
        profile = CalibrationPlanProfile.from_dict(payload)
        profile.name = _normalize_profile_name(profile.name)
        if set_default is not None:
            profile.is_default = bool(set_default)
        return self.save_profile(profile)

    def _load_index(self) -> dict[str, Any]:
        if not self.index_path.exists():
            return {
                "version": 1,
                "default_profile_name": None,
                "profiles": {},
            }
        try:
            payload = json.loads(self.index_path.read_text(encoding="utf-8"))
        except Exception:
            return {
                "version": 1,
                "default_profile_name": None,
                "profiles": {},
            }
        if not isinstance(payload, dict):
            return {
                "version": 1,
                "default_profile_name": None,
                "profiles": {},
            }
        profiles = payload.get("profiles")
        if not isinstance(profiles, dict):
            profiles = {}
        return {
            "version": int(payload.get("version", 1)),
            "default_profile_name": payload.get("default_profile_name"),
            "profiles": profiles,
        }

    def _save_index(self, index: dict[str, Any]) -> dict[str, Any]:
        payload = {
            "version": int(index.get("version", 1)),
            "default_profile_name": index.get("default_profile_name"),
            "profiles": dict(index.get("profiles", {})),
        }
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.index_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return payload

    def _build_filename(self, profile_name: str) -> str:
        digest = hashlib.sha1(profile_name.encode("utf-8")).hexdigest()[:8]
        return f"{_slugify(profile_name)}-{digest}.json"

    def _profile_path_from_entry(self, entry: dict[str, Any]) -> Path:
        return self.profiles_dir / str(entry.get("file", "")).strip()
