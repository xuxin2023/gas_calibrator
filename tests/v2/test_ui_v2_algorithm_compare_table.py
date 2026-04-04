from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.widgets.algorithm_compare_table import AlgorithmCompareTable

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import make_root


def test_algorithm_compare_table_renders_rows() -> None:
    root = make_root()
    try:
        table = AlgorithmCompareTable(root)
        table.render([{"algorithm": "amt", "source": "config", "status": "default", "note": "Current default"}])
        assert len(table.tree.get_children()) == 1
    finally:
        root.destroy()
