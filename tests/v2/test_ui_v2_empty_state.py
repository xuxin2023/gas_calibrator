from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.widgets.empty_state import EmptyState

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import make_root


def test_empty_state_renders_text() -> None:
    root = make_root()
    try:
        widget = EmptyState(root, title="暂无图表", message="数据稍后显示。")
        widget.render(title="仍为空", message="等待运行结果。")
        assert widget.title_var.get() == "仍为空"
        assert "等待运行结果" in widget.message_var.get()
    finally:
        root.destroy()
