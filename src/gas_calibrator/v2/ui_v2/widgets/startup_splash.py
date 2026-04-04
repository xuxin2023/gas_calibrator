from __future__ import annotations

import tkinter as tk
from tkinter import ttk

from ..i18n import t


class StartupSplash(tk.Toplevel):
    """Simple startup splash with progress feedback."""

    def __init__(self, parent: tk.Misc) -> None:
        super().__init__(parent)
        self.withdraw()
        self.title(t("widgets.startup.title"))
        self.resizable(False, False)
        self.progress_var = tk.DoubleVar(value=0.0)
        self.message_var = tk.StringVar(value=t("widgets.startup.starting"))
        self._build()
        self.deiconify()

    def _build(self) -> None:
        container = ttk.Frame(self, padding=16)
        container.grid(row=0, column=0, sticky="nsew")
        ttk.Label(container, text=t("widgets.startup.heading"), style="Title.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(container, textvariable=self.message_var, style="Muted.TLabel").grid(row=1, column=0, sticky="w", pady=(6, 10))
        ttk.Progressbar(container, variable=self.progress_var, maximum=100.0, length=280, mode="determinate").grid(row=2, column=0, sticky="ew")

    def set_progress(self, value: float, message: str) -> None:
        self.progress_var.set(max(0.0, min(100.0, float(value))))
        self.message_var.set(str(message or t("widgets.startup.loading")))
        self.update_idletasks()
