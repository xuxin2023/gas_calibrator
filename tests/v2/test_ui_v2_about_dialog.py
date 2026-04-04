from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.i18n import t
from gas_calibrator.v2.ui_v2.dialogs.about_dialog import AboutDialog
from gas_calibrator.v2.ui_v2.styles import apply_styles

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import make_root


def test_about_dialog_renders_app_identity() -> None:
    root = make_root()
    try:
        apply_styles(root)
        dialog = AboutDialog(
            root,
            app_info={
                "product_name": "Gas Calibrator V2 Cockpit",
                "version": "0.6.0-demo",
                "build": "local-dev",
                "vendor": "OpenAI / Industrial Calibration",
                "product_id": "gas-calibrator-v2",
            },
        )

        assert dialog.title() == t("dialogs.about.title")
        assert dialog.app_info["product_id"] == "gas-calibrator-v2"
    finally:
        root.destroy()
