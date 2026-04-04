from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Optional

from ..i18n import t


class MetricCard(ttk.Frame):
    """Simple metric card with title, value, and optional note."""

    def __init__(
        self,
        parent: tk.Misc,
        *,
        title: str,
        value: str = "--",
        note: str = "",
        value_var: Optional[tk.StringVar] = None,
        note_var: Optional[tk.StringVar] = None,
    ) -> None:
        super().__init__(parent, style="SoftCard.TFrame", padding=8)
        self.title_var = tk.StringVar(value=title or t("widgets.empty_state.title"))
        self.value_var = value_var or tk.StringVar(value=value)
        self.note_var = note_var or tk.StringVar(value=note)

        ttk.Label(self, textvariable=self.title_var, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(self, textvariable=self.value_var, style="Section.TLabel").grid(row=1, column=0, sticky="w")
        self.note_label = ttk.Label(self, textvariable=self.note_var, style="Muted.TLabel")
        self.note_label.grid(row=2, column=0, sticky="w")

    def set_value(self, value: str) -> None:
        self.value_var.set(str(value))

    def set_note(self, note: str) -> None:
        self.note_var.set(str(note))
