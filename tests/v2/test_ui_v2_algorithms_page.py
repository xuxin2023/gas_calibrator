from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.pages.algorithms_page import AlgorithmsPage

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import make_root


def test_algorithms_page_displays_snapshot() -> None:
    root = make_root()
    try:
        page = AlgorithmsPage(root)
        page.render(
            {
                "default_algorithm": "amt",
                "candidate_count": 3,
                "candidates": ["linear", "polynomial", "amt"],
                "coefficient_model": "ratio_poly_rt_p",
                "auto_select": True,
                "winner": {"winner": "amt", "status": "recommended", "reason": "Best current default"},
                "rows": [{"algorithm": "amt", "source": "config", "status": "default", "note": "Current default"}],
            }
        )
        assert page.page_scaffold is not None
        assert page.default_card.value_var.get() == "amt"
        assert page.winner_badge.winner_var.get() == "amt"
        assert len(page.table.tree.get_children()) == 1
    finally:
        root.destroy()
