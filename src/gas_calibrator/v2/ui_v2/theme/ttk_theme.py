from __future__ import annotations

import tkinter as tk
from tkinter import ttk

from .tokens import THEME, UITheme


def apply_styles(root: tk.Misc, theme: UITheme = THEME) -> UITheme:
    style = ttk.Style(root)
    try:
        style.theme_use("clam")
    except tk.TclError:
        pass

    root.configure(bg=theme.bg)

    style.configure(
        ".",
        background=theme.bg,
        foreground=theme.text,
        font=(theme.font_family, theme.font_size_md),
    )
    style.configure("TFrame", background=theme.bg)
    style.configure("Card.TFrame", background=theme.panel, relief="flat")
    style.configure("SoftCard.TFrame", background=theme.panel_soft, relief="flat")
    style.configure("TLabel", background=theme.bg, foreground=theme.text)
    style.configure(
        "Title.TLabel",
        background=theme.bg,
        foreground=theme.text,
        font=(theme.font_family, theme.font_size_xl, "bold"),
    )
    style.configure(
        "Section.TLabel",
        background=theme.panel,
        foreground=theme.text,
        font=(theme.font_family, theme.font_size_lg, "bold"),
    )
    style.configure("Muted.TLabel", background=theme.panel, foreground=theme.muted)
    style.configure(
        "Nav.TButton",
        padding=(theme.spacing_md, theme.spacing_sm),
        background=theme.panel,
        foreground=theme.text,
        borderwidth=0,
    )
    style.map("Nav.TButton", background=[("active", theme.accent_soft)])
    style.configure(
        "Accent.TButton",
        padding=(theme.spacing_md, theme.spacing_sm),
        background=theme.accent,
        foreground="#ffffff",
        borderwidth=0,
    )
    style.map("Accent.TButton", background=[("active", "#0b6971")])
    style.configure(
        "Treeview",
        background=theme.panel,
        fieldbackground=theme.panel,
        foreground=theme.text,
        rowheight=24,
        bordercolor=theme.line,
    )
    style.configure(
        "Treeview.Heading",
        background=theme.panel_soft,
        foreground=theme.text,
        relief="flat",
        font=(theme.font_family, theme.font_size_sm, "bold"),
    )
    style.map("Treeview.Heading", background=[("active", theme.accent_soft)])
    return theme

