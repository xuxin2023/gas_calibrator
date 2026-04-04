from __future__ import annotations

import tkinter as tk
from typing import Any, Callable


class ShortcutManager:
    """Centralized shell shortcut bindings."""

    def __init__(
        self,
        root: tk.Misc,
        *,
        shell: Any,
        run_controller: Any,
        open_preferences: Callable[[], None],
        open_about: Callable[[], None],
    ) -> None:
        self.root = root
        self.shell = shell
        self.run_controller = run_controller
        self.open_preferences = open_preferences
        self.open_about = open_about

    def bind_default_shortcuts(self) -> None:
        bindings = {
            "<Control-Key-1>": lambda _event: self.shell.show_page("run"),
            "<Control-Key-2>": lambda _event: self.shell.show_page("qc"),
            "<Control-Key-3>": lambda _event: self.shell.show_page("results"),
            "<Control-Key-4>": lambda _event: self.shell.show_page("devices"),
            "<Control-Key-5>": lambda _event: self.shell.show_page("algorithms"),
            "<Control-Key-6>": lambda _event: self.shell.show_page("reports"),
            "<Control-r>": lambda _event: self.shell._pages["run"]._on_start(),
            "<Control-s>": lambda _event: self.run_controller.stop(),
            "<Control-p>": lambda _event: self._toggle_pause(),
            "<Control-comma>": lambda _event: self.open_preferences(),
            "<F1>": lambda _event: self.open_about(),
        }
        for sequence, callback in bindings.items():
            self.root.bind_all(sequence, callback)

    def _toggle_pause(self) -> None:
        if str(self.shell.phase_var.get()).lower() == "paused":
            self.run_controller.resume()
        else:
            self.run_controller.pause()
