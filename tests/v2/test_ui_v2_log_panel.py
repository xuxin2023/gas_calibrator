from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.widgets.log_panel import LogPanel

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import make_root


def test_log_panel_renders_log_lines() -> None:
    root = make_root()
    try:
        panel = LogPanel(root)
        panel.set_logs(["line1", "line2"])
        assert "line1" in panel.text.get("1.0", "end")
        assert "line2" in panel.text.get("1.0", "end")
    finally:
        root.destroy()
