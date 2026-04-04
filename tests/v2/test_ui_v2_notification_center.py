from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.widgets.notification_center import NotificationCenter

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import make_root


def test_notification_center_renders_items() -> None:
    root = make_root()
    try:
        widget = NotificationCenter(root)
        widget.render({"items": [{"level": "info", "message": "已开始运行"}]})
        assert widget.listbox.size() == 1
        assert "提示" in widget.listbox.get(0)
        assert "已开始运行" in widget.listbox.get(0)
    finally:
        root.destroy()
