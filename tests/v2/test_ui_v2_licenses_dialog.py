from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.dialogs.licenses_dialog import LicensesDialog
from gas_calibrator.v2.ui_v2.i18n import t
from gas_calibrator.v2.ui_v2.styles import apply_styles

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import make_root


def test_licenses_dialog_displays_text() -> None:
    root = make_root()
    try:
        apply_styles(root)
        dialog = LicensesDialog(root, licenses_text="License A\nLicense B")

        assert dialog.title() == t("dialogs.licenses.title")
        assert "License A" in dialog.content_text
    finally:
        root.destroy()
