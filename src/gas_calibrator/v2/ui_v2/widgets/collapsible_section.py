from __future__ import annotations

import tkinter as tk
from tkinter import ttk


class CollapsibleSection(ttk.Frame):
    """Small reusable section with a hide/show body."""

    def __init__(
        self,
        parent: tk.Misc,
        *,
        title: str,
        summary: str = "",
        expanded: bool = False,
    ) -> None:
        super().__init__(parent, style="Card.TFrame")
        self.columnconfigure(0, weight=1)
        self._title = str(title or "")
        self._expanded = bool(expanded)

        header = ttk.Frame(self, style="Card.TFrame")
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(1, weight=1)

        self.toggle_button = ttk.Button(header, command=self.toggle, width=4)
        self.toggle_button.grid(row=0, column=0, sticky="w")
        self.title_label = ttk.Label(header, text=self._title, style="Section.TLabel")
        self.title_label.grid(row=0, column=1, sticky="w", padx=(6, 0))
        self.summary_var = tk.StringVar(value=str(summary or ""))
        self.summary_label = ttk.Label(header, textvariable=self.summary_var, style="Muted.TLabel")
        self.summary_label.grid(row=0, column=2, sticky="e", padx=(12, 0))

        self.body = ttk.Frame(self, style="Card.TFrame")
        self.body.grid(row=1, column=0, sticky="nsew", pady=(6, 0))
        self.body.columnconfigure(0, weight=1)

        self._sync_state()

    def set_summary(self, value: str) -> None:
        self.summary_var.set(str(value or ""))

    def set_title(self, value: str) -> None:
        self._title = str(value or "")
        self.title_label.configure(text=self._title)
        self._sync_state()

    def set_expanded(self, expanded: bool) -> None:
        self._expanded = bool(expanded)
        self._sync_state()

    def toggle(self) -> None:
        self._expanded = not self._expanded
        self._sync_state()

    @property
    def expanded(self) -> bool:
        return self._expanded

    def _sync_state(self) -> None:
        indicator = "[-]" if self._expanded else "[+]"
        self.toggle_button.configure(text=indicator)
        if self._expanded:
            self.body.grid()
        else:
            self.body.grid_remove()
