from __future__ import annotations

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_module(module_name: str, file_name: str):
    path = REPO_ROOT / file_name
    spec = spec_from_file_location(module_name, path)
    module = module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_root_launcher_delegates_arguments(monkeypatch) -> None:
    module = _load_module("test_run_gas_route_ratio_leak_check_entry", "run_gas_route_ratio_leak_check.py")
    captured: dict[str, object] = {}

    def _fake_main(argv):
        captured["argv"] = list(argv)
        return 0

    monkeypatch.setattr(module, "run_leak_check", _fake_main)

    result = module.main(["--allow-live-hardware", "--analyzer", "ga02"])

    assert result == 0
    assert captured["argv"] == ["--allow-live-hardware", "--analyzer", "ga02"]
