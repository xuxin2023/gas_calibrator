from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.widgets.busy_overlay import BusyOverlay

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import make_root


def test_busy_overlay_renders_state() -> None:
    root = make_root()
    try:
        widget = BusyOverlay(root)
        widget.render({"active": True, "message": "Loading..."})
        assert widget.visible is True
        assert widget.message_var.get() == "Loading..."
        widget.render({"active": False, "message": ""})
        assert widget.visible is False
    finally:
        root.destroy()
