from __future__ import annotations

import tkinter as tk
from tkinter import ttk

from ..i18n import t


class ReleaseNotesDialog(tk.Toplevel):
    """Display release notes loaded from a local file."""

    def __init__(self, parent: tk.Misc, *, notes_text: str) -> None:
        super().__init__(parent)
        self.withdraw()
        self.title(t("dialogs.release_notes.title"))
        self.geometry("880x560")
        self.content_text = str(notes_text or t("dialogs.release_notes.empty"))
        self._build()
        self.deiconify()

    def _build(self) -> None:
        container = ttk.Frame(self, padding=12)
        container.grid(row=0, column=0, sticky="nsew")
        self.rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)
        container.rowconfigure(0, weight=1)
        container.columnconfigure(0, weight=1)

        text = tk.Text(container, wrap="word", height=20)
        scrollbar = ttk.Scrollbar(container, orient="vertical", command=text.yview)
        text.configure(yscrollcommand=scrollbar.set)
        text.grid(row=0, column=0, sticky="nsew")
        scrollbar.grid(row=0, column=1, sticky="ns")
        text.insert("1.0", self.content_text)
        text.configure(state="disabled")
        ttk.Button(container, text=t("dialogs.common.close"), command=self.destroy).grid(row=1, column=0, sticky="e", pady=(12, 0))
