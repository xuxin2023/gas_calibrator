from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.dialogs.release_notes_dialog import ReleaseNotesDialog
from gas_calibrator.v2.ui_v2.i18n import t
from gas_calibrator.v2.ui_v2.styles import apply_styles

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import make_root


def test_release_notes_dialog_displays_text() -> None:
    root = make_root()
    try:
        apply_styles(root)
        dialog = ReleaseNotesDialog(root, notes_text="# Release Notes\n\n- Added diagnostics")

        assert dialog.title() == t("dialogs.release_notes.title")
        assert "Added diagnostics" in dialog.content_text
    finally:
        root.destroy()
