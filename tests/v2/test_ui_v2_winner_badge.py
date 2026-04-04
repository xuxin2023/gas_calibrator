from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.widgets.winner_badge import WinnerBadge

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import make_root


def test_winner_badge_renders_winner() -> None:
    root = make_root()
    try:
        widget = WinnerBadge(root)
        widget.render({"winner": "amt", "status": "recommended", "reason": "当前默认选择最优"})
        assert widget.winner_var.get() == "amt"
        assert widget.status_var.get() == "推荐"
        assert widget.reason_var.get() == "当前默认选择最优"
    finally:
        root.destroy()
