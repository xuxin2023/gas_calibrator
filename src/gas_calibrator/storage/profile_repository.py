"""File-backed JSON profile documents with atomic index updates."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
from typing import Any
from uuid import uuid4


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
class JsonProfileDocument:
    name: str
    payload: dict[str, Any]
    is_default: bool
    path: Path
    updated_at: str = ""


class JsonProfileRepository:
    """Store product-neutral JSON profile documents and one default pointer."""

    def __init__(self, base_dir: str | Path) -> None:
        self.base_dir = Path(base_dir)
        self.profiles_dir = self.base_dir / "profiles"
        self.index_path = self.base_dir / "index.json"

    def list_profiles(self) -> list[JsonProfileDocument]:
        index = self._load_index()
        documents = [
            document
            for name, entry in index["profiles"].items()
            if (document := self._load_document(name, entry, index)) is not None
        ]
        documents.sort(key=lambda item: (not item.is_default, item.name.lower()))
        return documents

    def load_profile(self, name: str) -> JsonProfileDocument | None:
        normalized_name = _normalize_profile_name(name)
        index = self._load_index()
        entry = index["profiles"].get(normalized_name)
        if not isinstance(entry, dict):
            return None
        return self._load_document(normalized_name, entry, index)

    def save_profile(
        self,
        *,
        name: str,
        payload: dict[str, Any],
        is_default: bool = False,
    ) -> JsonProfileDocument:
        normalized_name = _normalize_profile_name(name)
        index = self._load_index()
        profiles = dict(index["profiles"])
        entry = profiles.get(normalized_name)
        filename = (
            str(entry.get("file"))
            if isinstance(entry, dict) and entry.get("file")
            else self._build_filename(normalized_name)
        )
        stored_payload = dict(payload)
        stored_payload["name"] = normalized_name
        stored_payload["is_default"] = False

        path = self._safe_profile_path(filename)
        self._write_json_atomic(path, stored_payload)

        profiles[normalized_name] = {
            "file": filename,
            "updated_at": _utc_now_iso(),
        }
        index["profiles"] = profiles
        if is_default:
            index["default_profile_name"] = normalized_name
        self._save_index(index)

        loaded = self.load_profile(normalized_name)
        if loaded is None:
            raise RuntimeError(f"failed to reload saved profile: {normalized_name}")
        return loaded

    def delete_profile(self, name: str) -> bool:
        normalized_name = _normalize_profile_name(name)
        index = self._load_index()
        profiles = dict(index["profiles"])
        entry = profiles.pop(normalized_name, None)
        if not isinstance(entry, dict):
            return False

        index["profiles"] = profiles
        if index.get("default_profile_name") == normalized_name:
            index["default_profile_name"] = None
        self._save_index(index)

        try:
            path = self._safe_profile_path(str(entry.get("file", "")).strip())
        except ValueError:
            return True
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        return True

    def set_default_profile(self, name: str) -> JsonProfileDocument:
        normalized_name = _normalize_profile_name(name)
        index = self._load_index()
        if normalized_name not in index["profiles"]:
            raise ValueError(f"profile not found: {normalized_name}")
        index["default_profile_name"] = normalized_name
        self._save_index(index)
        profile = self.load_profile(normalized_name)
        if profile is None:
            raise RuntimeError(f"failed to reload default profile: {normalized_name}")
        return profile

    def get_default_profile(self) -> JsonProfileDocument | None:
        default_name = self._load_index().get("default_profile_name")
        if not default_name:
            return None
        return self.load_profile(str(default_name))

    def export_profile(self, name: str, destination: str | Path) -> Path:
        document = self.load_profile(name)
        if document is None:
            raise ValueError(f"profile not found: {name}")
        path = Path(destination)
        self._write_json_atomic(path, document.payload)
        return path

    def import_profile(
        self,
        source: str | Path,
        *,
        set_default: bool | None = None,
    ) -> JsonProfileDocument:
        path = Path(source)
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError(f"invalid profile payload: {path}")
        name = _normalize_profile_name(str(payload.get("name") or ""))
        is_default = bool(payload.get("is_default", False))
        if set_default is not None:
            is_default = bool(set_default)
        return self.save_profile(name=name, payload=payload, is_default=is_default)

    def _load_document(
        self,
        name: str,
        entry: dict[str, Any],
        index: dict[str, Any],
    ) -> JsonProfileDocument | None:
        try:
            path = self._safe_profile_path(str(entry.get("file", "")).strip())
        except ValueError:
            return None
        if not path.is_file():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None
        if not isinstance(payload, dict):
            return None
        document_payload = dict(payload)
        document_payload["name"] = name
        is_default = name == index.get("default_profile_name")
        document_payload["is_default"] = is_default
        return JsonProfileDocument(
            name=name,
            payload=document_payload,
            is_default=is_default,
            path=path,
            updated_at=str(entry.get("updated_at", "")),
        )

    def _load_index(self) -> dict[str, Any]:
        if not self.index_path.exists():
            return self._empty_index()
        try:
            payload = json.loads(self.index_path.read_text(encoding="utf-8"))
        except Exception:
            return self._empty_index()
        if not isinstance(payload, dict):
            return self._empty_index()
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
        self._write_json_atomic(self.index_path, payload)
        return payload

    @staticmethod
    def _empty_index() -> dict[str, Any]:
        return {
            "version": 1,
            "default_profile_name": None,
            "profiles": {},
        }

    @staticmethod
    def _build_filename(profile_name: str) -> str:
        digest = hashlib.sha1(profile_name.encode("utf-8")).hexdigest()[:8]
        return f"{_slugify(profile_name)}-{digest}.json"

    def _safe_profile_path(self, filename: str) -> Path:
        if not filename:
            raise ValueError("profile filename is required")
        root = self.profiles_dir.resolve()
        candidate = (self.profiles_dir / filename).resolve()
        try:
            candidate.relative_to(root)
        except ValueError as exc:
            raise ValueError(f"profile path escapes repository: {filename}") from exc
        return candidate

    @staticmethod
    def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
        try:
            with temporary.open("x", encoding="utf-8", newline="\n") as handle:
                json.dump(payload, handle, ensure_ascii=False, indent=2)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
