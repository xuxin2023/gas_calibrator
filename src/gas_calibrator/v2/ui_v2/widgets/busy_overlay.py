from __future__ import annotations

import tkinter as tk

from ..i18n import t
from ..theme.tokens import THEME


class BusyOverlay(tk.Frame):
    """Visual busy overlay for run/export/loading states."""

    def __init__(self, parent: tk.Misc) -> None:
        super().__init__(parent, bg=THEME.overlay, bd=0, highlightthickness=0)
        self.message_var = tk.StringVar(value=t("common.working"))
        self.label = tk.Label(
            self,
            textvariable=self.message_var,
            bg=THEME.overlay,
            fg=THEME.overlay_text,
            font=(THEME.font_family, THEME.font_size_lg, "bold"),
            padx=16,
            pady=12,
        )
        self.label.place(relx=0.5, rely=0.5, anchor="center")
        self.visible = False
        self.place_forget()

    def render(self, snapshot: dict[str, object]) -> None:
        active = bool(snapshot.get("active", False))
        message = str(snapshot.get("message", t("common.working")) or t("common.working"))
        self.message_var.set(message)
        self.visible = active
        if active:
            self.place(relx=0.0, rely=0.0, relwidth=1.0, relheight=1.0)
            self.lift()
        else:
            self.place_forget()
