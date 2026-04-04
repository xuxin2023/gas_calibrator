from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.diagnostics.diagnostic_bundle_exporter import DiagnosticBundleExporter

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import build_fake_facade


def test_diagnostic_bundle_exporter_writes_redacted_bundle(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    facade.save_preferences({"last_config_path": r"C:\Users\alice\secret\config.json"})
    facade.log_ui(r"Device COM9 failed at C:\Users\alice\Desktop\capture.txt")
    exporter = DiagnosticBundleExporter(facade.runtime_paths)

    result = exporter.export(facade)

    assert result["ok"] is True
    bundle_dir = Path(result["bundle_dir"])
    assert (bundle_dir / "snapshot.json").exists()
    assert (bundle_dir / "preflight.json").exists()
    assert (bundle_dir / "logs.txt").exists()
    assert "<PORT>" in (bundle_dir / "logs.txt").read_text(encoding="utf-8")
    preferences_text = (bundle_dir / "preferences.json").read_text(encoding="utf-8")
    assert "C:\\Users\\alice\\secret\\config.json" not in preferences_text
