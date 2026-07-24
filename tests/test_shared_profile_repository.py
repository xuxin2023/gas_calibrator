from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from gas_calibrator.storage.profile_repository import JsonProfileRepository


def test_repository_preserves_profile_lifecycle_and_default_pointer(tmp_path: Path) -> None:
    repository = JsonProfileRepository(tmp_path / "profiles")

    alpha = repository.save_profile(
        name="alpha",
        payload={"name": "alpha", "description": "first"},
    )
    repository.save_profile(
        name="beta",
        payload={"name": "beta", "description": "second"},
    )
    selected = repository.set_default_profile("beta")

    reloaded = JsonProfileRepository(tmp_path / "profiles")
    listed = reloaded.list_profiles()
    default = reloaded.get_default_profile()

    assert alpha.payload["is_default"] is False
    assert selected.is_default is True
    assert [item.name for item in listed] == ["beta", "alpha"]
    assert default is not None and default.name == "beta"
    assert reloaded.delete_profile("beta") is True
    assert reloaded.get_default_profile() is None
    assert [item.name for item in reloaded.list_profiles()] == ["alpha"]


def test_repository_import_export_round_trip_preserves_default(tmp_path: Path) -> None:
    source = JsonProfileRepository(tmp_path / "source")
    source.save_profile(
        name="portable",
        payload={"name": "portable", "profile_version": "2.0"},
        is_default=True,
    )
    exported = source.export_profile("portable", tmp_path / "export" / "portable.json")

    target = JsonProfileRepository(tmp_path / "target")
    imported = target.import_profile(exported)

    assert imported.name == "portable"
    assert imported.is_default is True
    assert target.get_default_profile() is not None
    assert target.get_default_profile().payload["profile_version"] == "2.0"


def test_repository_rejects_index_paths_outside_profile_directory(tmp_path: Path) -> None:
    base_dir = tmp_path / "repository"
    outside = tmp_path / "outside.json"
    outside.write_text('{"name": "escape"}\n', encoding="utf-8")
    base_dir.mkdir(parents=True)
    (base_dir / "index.json").write_text(
        json.dumps(
            {
                "version": 1,
                "default_profile_name": "escape",
                "profiles": {
                    "escape": {
                        "file": "../../outside.json",
                        "updated_at": "2026-07-24T00:00:00+00:00",
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    repository = JsonProfileRepository(base_dir)

    assert repository.load_profile("escape") is None
    assert repository.list_profiles() == []
    assert repository.delete_profile("escape") is True
    assert outside.exists()


def test_failed_index_replace_preserves_previous_default_and_cleans_temp_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = JsonProfileRepository(tmp_path / "repository")
    repository.save_profile(name="alpha", payload={"name": "alpha"}, is_default=True)
    repository.save_profile(name="beta", payload={"name": "beta"})
    real_replace = os.replace

    def fail_index_replace(source: str | Path, destination: str | Path) -> None:
        if Path(destination) == repository.index_path:
            raise OSError("forced index replace failure")
        real_replace(source, destination)

    monkeypatch.setattr(
        "gas_calibrator.storage.profile_repository.os.replace",
        fail_index_replace,
    )

    with pytest.raises(OSError, match="forced index replace failure"):
        repository.set_default_profile("beta")

    reloaded = JsonProfileRepository(repository.base_dir)
    default = reloaded.get_default_profile()
    assert default is not None and default.name == "alpha"
    assert list(repository.base_dir.glob(".*.tmp")) == []
