from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Any, Callable

from ..i18n import t


class PreferencesDialog(tk.Toplevel):
    """Simple preferences editor for client-side settings."""

    def __init__(
        self,
        parent: tk.Misc,
        *,
        initial: dict[str, Any],
        on_save: Callable[[dict[str, Any]], dict[str, Any] | None],
    ) -> None:
        super().__init__(parent)
        self.withdraw()
        self.title(t("dialogs.preferences.title"))
        self.resizable(False, False)
        self.transient(parent.winfo_toplevel())
        self.on_save = on_save
        self.result: dict[str, Any] | None = None
        self.last_config_var = tk.StringVar(value=str(initial.get("last_config_path", "") or ""))
        self.simulation_var = tk.BooleanVar(value=bool(initial.get("simulation_default", False)))
        self.auto_feed_var = tk.BooleanVar(value=bool(initial.get("auto_start_feed", True)))
        self.screenshot_var = tk.StringVar(value=str(initial.get("screenshot_format", "png") or "png"))
        self.status_var = tk.StringVar(value=t("dialogs.preferences.status.ready"))
        self._build()
        self.deiconify()

    def _build(self) -> None:
        container = ttk.Frame(self, padding=12, style="Card.TFrame")
        container.grid(row=0, column=0, sticky="nsew")
        container.columnconfigure(1, weight=1)
        ttk.Label(container, text=t("dialogs.preferences.last_config"), style="Section.TLabel").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=4)
        ttk.Entry(container, textvariable=self.last_config_var, width=48).grid(row=0, column=1, sticky="ew", pady=4)
        ttk.Checkbutton(container, text=t("dialogs.preferences.simulation_default"), variable=self.simulation_var).grid(row=1, column=0, columnspan=2, sticky="w", pady=4)
        ttk.Checkbutton(container, text=t("dialogs.preferences.auto_start_feed"), variable=self.auto_feed_var).grid(row=2, column=0, columnspan=2, sticky="w", pady=4)
        ttk.Label(container, text=t("dialogs.preferences.screenshot_format"), style="Section.TLabel").grid(row=3, column=0, sticky="w", padx=(0, 8), pady=4)
        ttk.Combobox(container, textvariable=self.screenshot_var, values=("png", "txt"), state="readonly").grid(row=3, column=1, sticky="ew", pady=4)
        ttk.Label(container, textvariable=self.status_var, style="Muted.TLabel").grid(row=4, column=0, columnspan=2, sticky="w", pady=(8, 0))
        buttons = ttk.Frame(container, style="Card.TFrame")
        buttons.grid(row=5, column=0, columnspan=2, sticky="e", pady=(12, 0))
        ttk.Button(buttons, text=t("dialogs.common.cancel"), command=self.destroy).grid(row=0, column=0, padx=4)
        ttk.Button(buttons, text=t("dialogs.common.save"), style="Accent.TButton", command=self._save).grid(row=0, column=1, padx=4)

    def _save(self) -> None:
        payload = {
            "last_config_path": self.last_config_var.get().strip(),
            "simulation_default": bool(self.simulation_var.get()),
            "auto_start_feed": bool(self.auto_feed_var.get()),
            "screenshot_format": self.screenshot_var.get().strip() or "png",
        }
        result = self.on_save(payload) or payload
        self.result = dict(result)
        self.status_var.set(t("dialogs.preferences.status.saved"))
        self.destroy()
