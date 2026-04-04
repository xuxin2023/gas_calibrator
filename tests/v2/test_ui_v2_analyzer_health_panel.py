from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.widgets.analyzer_health_panel import AnalyzerHealthPanel

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import make_root


def test_analyzer_health_panel_renders_rows() -> None:
    root = make_root()
    try:
        widget = AnalyzerHealthPanel(root)
        widget.render({"rows": [{"analyzer": "gas_analyzer_0", "status": "online", "health": 95, "note": "stable"}]})
        assert len(widget.tree.get_children()) == 1
    finally:
        root.destroy()
