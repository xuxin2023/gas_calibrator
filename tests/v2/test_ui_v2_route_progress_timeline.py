from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.widgets.route_progress_timeline import RouteProgressTimeline

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import make_root


def test_route_progress_timeline_draws_steps() -> None:
    root = make_root()
    try:
        widget = RouteProgressTimeline(root)
        widget.pack(fill="both", expand=True)
        root.update_idletasks()
        widget.render({"route": "co2", "route_phase": "co2_route", "points_completed": 1, "points_total": 2, "steps": ["H2O", "CO2", "Finalize"]})
        root.update_idletasks()
        assert widget.canvas.find_all()
        assert "气路" in widget.detail_var.get()
        assert "气路执行" in widget.detail_var.get()
    finally:
        root.destroy()
