from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.widgets.residual_chart import ResidualChart

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import make_root


def test_residual_chart_draws_multi_algorithm_series() -> None:
    root = make_root()
    try:
        widget = ResidualChart(root)
        widget.pack(fill="both", expand=True)
        root.update_idletasks()
        widget.render(
            {
                "series": [
                    {"algorithm": "amt", "residuals": [0.5, -0.3, 0.1]},
                    {"algorithm": "linear", "residuals": [0.8, -0.1, 0.2]},
                ]
            }
        )
        root.update_idletasks()
        assert widget.canvas.find_all()
    finally:
        root.destroy()
