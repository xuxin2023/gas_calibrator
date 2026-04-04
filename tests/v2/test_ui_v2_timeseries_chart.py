from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.widgets.timeseries_chart import TimeSeriesChart

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import make_root


def test_timeseries_chart_draws_series() -> None:
    root = make_root()
    try:
        widget = TimeSeriesChart(root, max_points=4)
        widget.pack(fill="both", expand=True)
        root.update_idletasks()
        widget.set_series({"temperature_c": [24.8, 25.0, 25.2], "pressure_hpa": [998.0, 999.5, 1000.0]})
        root.update_idletasks()
        assert widget.canvas.find_all()
        widget.append({"temperature_c": 25.4})
        root.update_idletasks()
        assert len(widget._series["temperature_c"]) <= 4
    finally:
        root.destroy()
