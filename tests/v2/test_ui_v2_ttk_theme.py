from pathlib import Path
import sys
from tkinter import ttk

from gas_calibrator.v2.ui_v2.theme.ttk_theme import apply_styles

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import make_root


def test_ttk_theme_applies_styles() -> None:
    root = make_root()
    try:
        theme = apply_styles(root)
        style = ttk.Style(root)
        assert theme.bg.startswith("#")
        assert style.lookup("Card.TFrame", "background")
    finally:
        root.destroy()
