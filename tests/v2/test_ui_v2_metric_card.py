from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.widgets.metric_card import MetricCard

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import make_root


def test_metric_card_updates_value_and_note() -> None:
    root = make_root()
    try:
        card = MetricCard(root, title="阶段", value="空闲", note="就绪")
        assert card.value_var.get() == "空闲"
        card.set_value("运行中")
        card.set_note("气路执行")
        assert card.value_var.get() == "运行中"
        assert card.note_var.get() == "气路执行"
    finally:
        root.destroy()
