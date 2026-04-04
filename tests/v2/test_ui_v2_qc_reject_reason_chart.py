from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.widgets.qc_reject_reason_chart import QCRejectReasonChart

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import make_root


def test_qc_reject_reason_chart_draws_counts() -> None:
    root = make_root()
    try:
        widget = QCRejectReasonChart(root)
        widget.pack(fill="both", expand=True)
        root.update_idletasks()
        widget.render({"rows": [{"reason": "outlier_ratio_too_high", "count": 2}]})
        root.update_idletasks()
        assert widget.canvas.find_all()
    finally:
        root.destroy()
