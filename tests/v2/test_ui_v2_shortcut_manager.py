from pathlib import Path
import sys
from types import SimpleNamespace
import tkinter as tk

from gas_calibrator.v2.ui_v2.controllers.shortcut_manager import ShortcutManager

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import make_root


def test_shortcut_manager_binds_and_dispatches_actions() -> None:
    root = make_root()
    pages: list[str] = []
    calls = {"start": 0, "stop": 0, "pause": 0, "resume": 0, "preferences": 0, "about": 0}
    try:
        run_page = SimpleNamespace(_on_start=lambda: calls.__setitem__("start", calls["start"] + 1))
        shell = SimpleNamespace(
            _pages={"run": run_page},
            phase_var=tk.StringVar(master=root, value="idle"),
            show_page=lambda name: pages.append(name),
        )
        controller = SimpleNamespace(
            stop=lambda: calls.__setitem__("stop", calls["stop"] + 1),
            pause=lambda: calls.__setitem__("pause", calls["pause"] + 1),
            resume=lambda: calls.__setitem__("resume", calls["resume"] + 1),
        )
        manager = ShortcutManager(
            root,
            shell=shell,
            run_controller=controller,
            open_preferences=lambda: calls.__setitem__("preferences", calls["preferences"] + 1),
            open_about=lambda: calls.__setitem__("about", calls["about"] + 1),
        )
        manager.bind_default_shortcuts()
        root.update()

        assert root.bind_all("<Control-Key-1>")
        assert root.bind_all("<Control-r>")
        assert root.bind_all("<Control-s>")
        assert root.bind_all("<Control-p>")
        assert root.bind_all("<Control-comma>")
        assert root.bind_all("<F1>")

        shell.show_page("run")
        shell._pages["run"]._on_start()
        controller.stop()
        manager._toggle_pause()
        shell.phase_var.set("paused")
        manager._toggle_pause()
        manager.open_preferences()
        manager.open_about()

        assert "run" in pages
        assert calls["start"] >= 1
        assert calls["stop"] >= 1
        assert calls["pause"] >= 1
        assert calls["resume"] >= 1
        assert calls["preferences"] >= 1
        assert calls["about"] >= 1
    finally:
        root.destroy()
