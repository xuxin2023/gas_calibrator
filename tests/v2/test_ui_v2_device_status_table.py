from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.widgets.device_status_table import DeviceStatusTable

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import make_root


def test_device_status_table_renders_rows() -> None:
    root = make_root()
    try:
        table = DeviceStatusTable(root)
        table.render([{"name": "gas_analyzer_0", "status": "online", "port": "COM2"}])
        assert len(table.tree.get_children()) == 1
    finally:
        root.destroy()
