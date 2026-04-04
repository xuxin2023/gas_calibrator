from __future__ import annotations

import tkinter as tk
from tkinter import ttk

from ..i18n import t


class EmptyState(ttk.Frame):
    """Friendly empty-state prompt for panels without data."""

    def __init__(
        self,
        parent: tk.Misc,
        *,
        title: str | None = None,
        message: str | None = None,
    ) -> None:
        super().__init__(parent, style="Card.TFrame", padding=12)
        self.title_var = tk.StringVar(value=title or t("widgets.empty_state.title"))
        self.message_var = tk.StringVar(value=message or t("widgets.empty_state.message"))
        ttk.Label(self, textvariable=self.title_var, style="Section.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(
            self,
            textvariable=self.message_var,
            style="Muted.TLabel",
            wraplength=320,
            justify="left",
        ).grid(row=1, column=0, sticky="w", pady=(6, 0))

    def render(self, *, title: str | None = None, message: str | None = None) -> None:
        if title is not None:
            self.title_var.set(str(title))
        if message is not None:
            self.message_var.set(str(message))
