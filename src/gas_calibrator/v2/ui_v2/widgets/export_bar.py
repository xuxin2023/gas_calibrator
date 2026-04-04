from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Callable, Optional

from ..i18n import t


class ExportBar(ttk.Frame):
    """Action bar for exporting UI-visible artifacts."""

    def __init__(
        self,
        parent: tk.Misc,
        *,
        on_export_json: Optional[Callable[[], tuple[bool, str] | dict[str, object]]] = None,
        on_export_csv: Optional[Callable[[], tuple[bool, str] | dict[str, object]]] = None,
        on_export_all: Optional[Callable[[], tuple[bool, str] | dict[str, object]]] = None,
        on_export_review_manifest: Optional[Callable[[], tuple[bool, str] | dict[str, object]]] = None,
    ) -> None:
        super().__init__(parent, style="Card.TFrame", padding=8)
        self._on_export_json = on_export_json
        self._on_export_csv = on_export_csv
        self._on_export_all = on_export_all
        self._on_export_review_manifest = on_export_review_manifest
        self.status_var = tk.StringVar(value=t("widgets.export_bar.ready"))
        self.formats_var = tk.StringVar(value=t("widgets.export_bar.formats", formats="json, csv, all"))
        ttk.Label(self, text=t("widgets.export_bar.title"), style="Section.TLabel").grid(row=0, column=0, sticky="w", padx=(0, 12))
        ttk.Button(self, text="JSON", command=self.export_json).grid(row=0, column=1, padx=4)
        ttk.Button(self, text="CSV", command=self.export_csv).grid(row=0, column=2, padx=4)
        ttk.Button(self, text=t("widgets.export_bar.export_all"), style="Accent.TButton", command=self.export_all).grid(row=0, column=3, padx=4)
        ttk.Button(
            self,
            text=t("widgets.export_bar.review_manifest"),
            command=self.export_review_manifest,
        ).grid(row=0, column=4, padx=(12, 0))
        ttk.Label(self, textvariable=self.formats_var, style="Muted.TLabel").grid(row=1, column=0, sticky="w", columnspan=3, pady=(6, 0))
        ttk.Label(self, textvariable=self.status_var, style="Muted.TLabel").grid(row=1, column=3, sticky="e", columnspan=2, pady=(6, 0))

    def bind_actions(
        self,
        *,
        on_export_json: Optional[Callable[[], tuple[bool, str] | dict[str, object]]] = None,
        on_export_csv: Optional[Callable[[], tuple[bool, str] | dict[str, object]]] = None,
        on_export_all: Optional[Callable[[], tuple[bool, str] | dict[str, object]]] = None,
        on_export_review_manifest: Optional[Callable[[], tuple[bool, str] | dict[str, object]]] = None,
    ) -> None:
        self._on_export_json = on_export_json
        self._on_export_csv = on_export_csv
        self._on_export_all = on_export_all
        self._on_export_review_manifest = on_export_review_manifest

    def render(self, snapshot: dict[str, object]) -> None:
        formats = [str(item) for item in list(snapshot.get("available_formats", []) or [])]
        self.formats_var.set(t("widgets.export_bar.formats", formats=", ".join(formats) if formats else t("common.none")))
        self.status_var.set(str(snapshot.get("last_export_message", t("widgets.export_bar.ready")) or t("widgets.export_bar.ready")))

    def export_json(self) -> None:
        self._run(self._on_export_json)

    def export_csv(self) -> None:
        self._run(self._on_export_csv)

    def export_all(self) -> None:
        self._run(self._on_export_all)

    def export_review_manifest(self) -> None:
        self._run(self._on_export_review_manifest)

    def _run(self, callback: Optional[Callable[[], tuple[bool, str] | dict[str, object]]]) -> None:
        if callback is None:
            self.status_var.set(t("widgets.export_bar.unavailable"))
            return
        try:
            result = callback()
        except Exception as exc:
            self.status_var.set(t("widgets.export_bar.failed", message=exc))
            return
        if isinstance(result, dict):
            ok = bool(result.get("ok", False))
            message = str(result.get("message", t("widgets.export_bar.done")) or t("widgets.export_bar.done"))
        else:
            ok, message = result
        self.status_var.set(message if ok else t("widgets.export_bar.failed_short", message=message))
