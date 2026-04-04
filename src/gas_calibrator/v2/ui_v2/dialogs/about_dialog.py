from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Any

from ..i18n import t


class AboutDialog(tk.Toplevel):
    """About dialog for product identity and build information."""

    def __init__(self, parent: tk.Misc, *, app_info: dict[str, Any]) -> None:
        super().__init__(parent)
        self.withdraw()
        self.title(t("dialogs.about.title"))
        self.resizable(False, False)
        self.transient(parent.winfo_toplevel())
        self.app_info = dict(app_info or {})
        self._build()
        self.deiconify()

    def _build(self) -> None:
        container = ttk.Frame(self, padding=12, style="Card.TFrame")
        container.grid(row=0, column=0, sticky="nsew")
        lines = [
            f"{t('dialogs.about.product')}: {self.app_info.get('product_name', '--')}",
            f"{t('dialogs.about.version')}: {self.app_info.get('version', '--')}",
            f"{t('dialogs.about.build')}: {self.app_info.get('build', '--')}",
            f"{t('dialogs.about.vendor')}: {self.app_info.get('vendor', '--')}",
            f"{t('dialogs.about.product_id')}: {self.app_info.get('product_id', '--')}",
        ]
        ttk.Label(container, text="\n".join(lines), style="Section.TLabel", justify="left").grid(row=0, column=0, sticky="w")
        ttk.Button(container, text=t("dialogs.common.close"), command=self.destroy).grid(row=1, column=0, sticky="e", pady=(12, 0))
