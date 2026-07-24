from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from ...storage.profile_repository import JsonProfileDocument, JsonProfileRepository
from ..domain.plan_models import CalibrationPlanProfile


def _normalize_profile_name(name: str) -> str:
    normalized = str(name or "").strip()
    if not normalized:
        raise ValueError("profile name is required")
    return normalized


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
        self.repository = JsonProfileRepository(self.base_dir)

    def list_profiles(self) -> list[ProfileSummary]:
        summaries: list[ProfileSummary] = []
        for document in self.repository.list_profiles():
            profile = self._profile_from_document(document)
            summaries.append(
                ProfileSummary(
                    name=profile.name,
                    profile_version=str(getattr(profile, "profile_version", "1.0") or "1.0"),
                    description=profile.description,
                    is_default=document.is_default,
                    path=str(document.path),
                    updated_at=document.updated_at,
                )
            )
        return summaries

    def load_profile(self, name: str) -> CalibrationPlanProfile | None:
        document = self.repository.load_profile(name)
        return None if document is None else self._profile_from_document(document)

    def save_profile(self, profile: CalibrationPlanProfile) -> CalibrationPlanProfile:
        normalized_name = _normalize_profile_name(profile.name)
        payload = profile.to_dict()
        payload["name"] = normalized_name
        document = self.repository.save_profile(
            name=normalized_name,
            payload=payload,
            is_default=bool(profile.is_default),
        )
        return self._profile_from_document(document)

    def delete_profile(self, name: str) -> bool:
        return self.repository.delete_profile(name)

    def set_default_profile(self, name: str) -> CalibrationPlanProfile:
        return self._profile_from_document(self.repository.set_default_profile(name))

    def get_default_profile(self) -> CalibrationPlanProfile | None:
        document = self.repository.get_default_profile()
        return None if document is None else self._profile_from_document(document)

    def export_profile(self, name: str, destination: Path) -> Path:
        return self.repository.export_profile(name, destination)

    def import_profile(
        self,
        source: Path,
        *,
        set_default: Optional[bool] = None,
    ) -> CalibrationPlanProfile:
        document = self.repository.import_profile(source, set_default=set_default)
        return self._profile_from_document(document)

    @staticmethod
    def _profile_from_document(document: JsonProfileDocument) -> CalibrationPlanProfile:
        profile = CalibrationPlanProfile.from_dict(document.payload)
        profile.name = document.name
        profile.is_default = document.is_default
        return profile
