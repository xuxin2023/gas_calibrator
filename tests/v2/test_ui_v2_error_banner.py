from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.widgets.error_banner import ErrorBanner

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import make_root


def test_error_banner_shows_and_hides() -> None:
    root = make_root()
    try:
        widget = ErrorBanner(root)
        widget.grid(row=0, column=0)
        widget.render({"visible": True, "message": "fatal"})
        assert widget.visible is True
        assert widget.message_var.get() == "fatal"
        widget.render({"visible": False, "message": ""})
        assert widget.visible is False
    finally:
        root.destroy()
