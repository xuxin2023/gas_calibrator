from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.utils.screenshot import export_widget_screenshot

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import make_root


def test_screenshot_tool_exports_fallback_file(tmp_path: Path) -> None:
    root = make_root()
    try:
        root.update_idletasks()
        path = export_widget_screenshot(root, tmp_path / "capture.txt")
        assert path.exists()
    finally:
        root.destroy()
