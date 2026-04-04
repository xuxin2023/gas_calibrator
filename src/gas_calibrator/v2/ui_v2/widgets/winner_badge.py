from __future__ import annotations

import tkinter as tk
from tkinter import ttk

from ..i18n import display_winner_status, t


class WinnerBadge(ttk.Frame):
    """Highlighted winner display for algorithm selection."""

    def __init__(self, parent: tk.Misc, *, title: str | None = None) -> None:
        super().__init__(parent, style="Card.TFrame", padding=10)
        self.title_var = tk.StringVar(value=title or t("widgets.winner.title"))
        self.winner_var = tk.StringVar(value="--")
        self.status_var = tk.StringVar(value=t("widgets.winner.pending"))
        self.reason_var = tk.StringVar(value=t("widgets.winner.no_recommendation"))
        ttk.Label(self, textvariable=self.title_var, style="Muted.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(self, textvariable=self.winner_var, style="Title.TLabel").grid(row=1, column=0, sticky="w", pady=(2, 0))
        ttk.Label(self, textvariable=self.status_var, style="Section.TLabel").grid(row=2, column=0, sticky="w", pady=(4, 0))
        ttk.Label(self, textvariable=self.reason_var, style="Muted.TLabel", wraplength=420, justify="left").grid(row=3, column=0, sticky="w", pady=(6, 0))

    def render(self, snapshot: dict[str, object]) -> None:
        self.winner_var.set(str(snapshot.get("winner", "--") or "--"))
        status = snapshot.get("status")
        self.status_var.set(
            str(
                snapshot.get("status_display")
                or display_winner_status(status, default=t("widgets.winner.pending"))
                or t("widgets.winner.pending")
            )
        )
        self.reason_var.set(
            str(
                snapshot.get("reason_display")
                or snapshot.get("reason")
                or t("widgets.winner.no_recommendation")
            )
        )
