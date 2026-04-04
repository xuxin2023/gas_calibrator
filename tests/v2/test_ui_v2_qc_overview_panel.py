from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.widgets.qc_overview_panel import QCOverviewPanel

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import make_root


def test_qc_overview_panel_renders_metrics() -> None:
    root = make_root()
    try:
        widget = QCOverviewPanel(root)
        widget.pack(fill="both", expand=True)
        root.update_idletasks()
        widget.render({"score": 0.81, "grade": "B", "valid_points": 1, "invalid_points": 1, "total_points": 2})
        assert widget.score_card.value_var.get() == "0.81"
        assert widget.valid_card.value_var.get() == "1"
        assert widget.invalid_card.value_var.get() == "1"
        assert "有效" in widget.summary_var.get()
        assert widget.canvas.find_all()
    finally:
        root.destroy()
