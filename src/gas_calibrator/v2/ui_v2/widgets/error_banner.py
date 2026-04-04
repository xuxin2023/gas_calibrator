from __future__ import annotations

import tkinter as tk

from ..theme.tokens import THEME


class ErrorBanner(tk.Frame):
    """Global error banner for shell-level failures and warnings."""

    def __init__(self, parent: tk.Misc) -> None:
        super().__init__(parent, bg=THEME.error_bg, highlightthickness=1, highlightbackground=THEME.danger)
        self.message_var = tk.StringVar(value="")
        self.label = tk.Label(
            self,
            textvariable=self.message_var,
            bg=THEME.error_bg,
            fg=THEME.danger,
            anchor="w",
            justify="left",
            padx=12,
            pady=8,
        )
        self.label.pack(fill="x")
        self.visible = False
        self.grid_remove()

    def render(self, snapshot: dict[str, object]) -> None:
        visible = bool(snapshot.get("visible", False))
        message = str(snapshot.get("message", "") or "")
        self.message_var.set(message)
        self.visible = visible and bool(message)
        if self.visible:
            self.grid()
        else:
            self.grid_remove()

