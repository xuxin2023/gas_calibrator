from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.styles import apply_styles
from gas_calibrator.v2.ui_v2.widgets.startup_splash import StartupSplash

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import make_root


def test_startup_splash_updates_progress() -> None:
    root = make_root()
    try:
        apply_styles(root)
        splash = StartupSplash(root)
        splash.set_progress(65, "正在加载驾驶舱...")

        assert float(splash.progress_var.get()) == 65.0
        assert splash.title() == "正在启动气体校准 V2"
        assert splash.message_var.get() == "正在加载驾驶舱..."
        splash.destroy()
    finally:
        root.destroy()
