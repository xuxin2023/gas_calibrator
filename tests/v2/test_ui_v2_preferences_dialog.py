from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.dialogs.preferences_dialog import PreferencesDialog
from gas_calibrator.v2.ui_v2.i18n import t
from gas_calibrator.v2.ui_v2.styles import apply_styles

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import make_root


def test_preferences_dialog_collects_values_and_saves() -> None:
    root = make_root()
    saved_payload = {}
    try:
        apply_styles(root)
        dialog = PreferencesDialog(
            root,
            initial={"last_config_path": "old.json", "simulation_default": False, "auto_start_feed": True, "screenshot_format": "png"},
            on_save=lambda payload: saved_payload.update(payload) or payload,
        )
        assert dialog.title() == t("dialogs.preferences.title")
        dialog.last_config_var.set("new.json")
        dialog.simulation_var.set(True)
        dialog.screenshot_var.set("txt")

        dialog._save()

        assert saved_payload["last_config_path"] == "new.json"
        assert saved_payload["simulation_default"] is True
        assert saved_payload["screenshot_format"] == "txt"
        assert dialog.result is not None
    finally:
        root.destroy()
