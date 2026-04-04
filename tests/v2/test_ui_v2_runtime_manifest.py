from pathlib import Path

from gas_calibrator.v2.ui_v2.packaging.runtime_manifest import build_runtime_manifest


def test_runtime_manifest_reports_required_ui_files() -> None:
    ui_root = Path(__file__).resolve().parents[2] / "src" / "gas_calibrator" / "v2" / "ui_v2"

    manifest = build_runtime_manifest(ui_root)

    assert manifest["required_files"]
    assert any(item["path"] == "app.py" and item["present"] for item in manifest["required_files"])
