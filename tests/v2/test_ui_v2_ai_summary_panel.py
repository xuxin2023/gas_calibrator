from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.widgets.ai_summary_panel import AISummaryPanel

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import make_root


def test_ai_summary_panel_renders_text() -> None:
    root = make_root()
    try:
        panel = AISummaryPanel(root)
        panel.set_text("# AI Run Summary\nStable")
        assert "Stable" in panel.text.get("1.0", "end")
    finally:
        root.destroy()
