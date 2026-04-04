from __future__ import annotations

import tkinter as tk
from tkinter import ttk

from ..i18n import t


class AISummaryPanel(ttk.Frame):
    """Readonly panel for AI-generated summary text."""

    def __init__(self, parent: tk.Misc, *, title: str | None = None) -> None:
        super().__init__(parent, style="Card.TFrame", padding=0)
        self.rowconfigure(1, weight=1)
        self.columnconfigure(0, weight=1)
        ttk.Label(self, text=title or t("widgets.ai_summary.title"), style="Section.TLabel").grid(row=0, column=0, sticky="w", pady=(0, 6))
        self.text = tk.Text(self, height=8, wrap="word")
        self.text.grid(row=1, column=0, sticky="nsew")
        self.scrollbar = ttk.Scrollbar(self, orient="vertical", command=self.text.yview)
        self.scrollbar.grid(row=1, column=1, sticky="ns", padx=(6, 0))
        self.text.configure(yscrollcommand=self.scrollbar.set)
        self.text.configure(state="disabled")

    def set_text(self, value: str) -> None:
        self.text.configure(state="normal")
        self.text.delete("1.0", "end")
        self.text.insert("1.0", (value or "").strip() + "\n")
        self.text.configure(state="disabled")

    def get(self, start: str, end: str) -> str:
        """Compatibility passthrough for tests and read-only consumers."""
        return self.text.get(start, end)
