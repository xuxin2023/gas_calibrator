from pathlib import Path

from gas_calibrator.v2.ui_v2.runtime.build_info_loader import load_build_info
from gas_calibrator.v2.ui_v2.utils.app_info import APP_INFO


def test_build_info_loader_falls_back_when_file_missing(tmp_path: Path) -> None:
    info = load_build_info(tmp_path / "missing.json")

    assert info["product_name"] == APP_INFO.product_name
    assert info["version"]


def test_build_info_loader_overrides_defaults_from_file(tmp_path: Path) -> None:
    source = tmp_path / "build_info.json"
    source.write_text('{"version":"9.9.9","build":"release"}', encoding="utf-8")

    info = load_build_info(source)

    assert info["version"] == "9.9.9"
    assert info["build"] == "release"
