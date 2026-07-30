from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk
from typing import Any, Callable

from ...certificate_metrics_registry import CertificateMetricsRegistry
from ..page_i18n import translator_for
from ..scrollable_page_frame import ScrollablePageFrame


class CertificateMetricsPage(ttk.Frame):
    """Editable certificate metadata, isolated from calibration execution."""

    _FIELD_ROWS = (
        ("asset_id", "asset_id"),
        ("asset_name", "asset_name"),
        ("asset_type", "asset_type"),
        ("measurand", "measurand"),
        ("certificate_id", "certificate_id"),
        ("certificate_version", "certificate_version"),
        ("nominal_value", "nominal_value"),
        ("certified_value", "certified_value"),
        ("unit", "unit"),
        ("standard_uncertainty", "standard_uncertainty"),
        ("expanded_uncertainty", "expanded_uncertainty"),
        ("coverage_factor", "coverage_factor"),
        ("uncertainty_unit", "uncertainty_unit"),
        ("cylinder_serial_number", "cylinder_serial_number"),
        ("manufacturer", "manufacturer"),
        ("balance_gas", "balance_gas"),
        ("gas_matrix", "gas_matrix"),
        ("preparation_method", "preparation_method"),
        ("issue_date", "issue_date"),
        ("valid_from", "valid_from"),
        ("valid_until", "valid_until"),
        ("traceability_chain", "traceability_chain"),
        ("evidence_file_path", "evidence_file_path"),
        ("evidence_file_sha256", "evidence_file_sha256"),
        ("notes", "notes"),
    )

    def __init__(
        self,
        parent: tk.Misc,
        *,
        registry: CertificateMetricsRegistry,
        locale: str = "zh_CN",
        translate: Callable[..., str] | None = None,
    ) -> None:
        super().__init__(parent, style="Card.TFrame")
        self.registry = registry
        self._t = translate or translator_for(locale)
        self.current_record_id = ""
        self.form_vars = {
            field: tk.StringVar(value="") for field, _ in self._FIELD_ROWS
        }
        self.status_var = tk.StringVar(
            value=self._t("pages.certificate_metrics.status.ready")
        )
        self.count_var = tk.StringVar(value="")
        self._build()
        self.refresh_records()

    def render(self, snapshot: dict[str, Any]) -> None:
        _ = snapshot

    def _build(self) -> None:
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)
        self.page_scaffold = ScrollablePageFrame(
            self,
            padding=16,
            canvas_background="#0c1927",
        )
        self.page_scaffold.grid(row=0, column=0, sticky="nsew")
        body = self.page_scaffold.content
        body.columnconfigure(1, weight=1)
        body.rowconfigure(2, weight=1)

        ttk.Label(
            body,
            text=self._t("pages.certificate_metrics.title"),
            style="Title.TLabel",
        ).grid(row=0, column=0, columnspan=2, sticky="w")
        ttk.Label(
            body,
            text=self._t("pages.certificate_metrics.boundary"),
            style="Muted.TLabel",
            wraplength=1400,
            justify="left",
        ).grid(row=1, column=0, columnspan=2, sticky="ew", pady=(6, 14))

        left = ttk.LabelFrame(
            body,
            text=self._t("pages.certificate_metrics.records.title"),
            style="Card.TFrame",
            padding=10,
        )
        left.grid(row=2, column=0, sticky="nsw", padx=(0, 14))
        left.columnconfigure(0, weight=1)
        left.rowconfigure(1, weight=1)
        ttk.Label(left, textvariable=self.count_var, style="Muted.TLabel").grid(
            row=0, column=0, sticky="w", pady=(0, 6)
        )
        self.records_tree = ttk.Treeview(
            left,
            columns=("asset", "certificate", "value", "state", "revision"),
            show="headings",
            height=25,
        )
        self.records_tree.grid(row=1, column=0, sticky="nsew")
        self.records_tree.bind("<<TreeviewSelect>>", self._on_record_selected)
        scrollbar = ttk.Scrollbar(
            left, orient="vertical", command=self.records_tree.yview
        )
        scrollbar.grid(row=1, column=1, sticky="ns")
        self.records_tree.configure(yscrollcommand=scrollbar.set)
        for key, label_key, width, anchor in (
            ("asset", "asset", 170, "w"),
            ("certificate", "certificate", 150, "w"),
            ("value", "value", 110, "center"),
            ("state", "state", 110, "center"),
            ("revision", "revision", 60, "center"),
        ):
            self.records_tree.heading(
                key,
                text=self._t(
                    f"pages.certificate_metrics.records.column.{label_key}"
                ),
            )
            self.records_tree.column(key, width=width, anchor=anchor)
        ttk.Button(
            left,
            text=self._t("pages.certificate_metrics.actions.new"),
            command=self.new_record,
        ).grid(row=2, column=0, columnspan=2, sticky="ew", pady=(8, 0))

        right = ttk.LabelFrame(
            body,
            text=self._t("pages.certificate_metrics.form.title"),
            style="Card.TFrame",
            padding=12,
        )
        right.grid(row=2, column=1, sticky="nsew")
        right.columnconfigure(1, weight=1)
        right.columnconfigure(3, weight=1)

        for index, (field, label_key) in enumerate(self._FIELD_ROWS):
            group = index % 2
            row = index // 2
            label_column = group * 2
            entry_column = label_column + 1
            ttk.Label(
                right,
                text=self._t(f"pages.certificate_metrics.field.{label_key}"),
                style="Muted.TLabel",
            ).grid(row=row, column=label_column, sticky="w", padx=(0, 6), pady=4)
            ttk.Entry(right, textvariable=self.form_vars[field]).grid(
                row=row,
                column=entry_column,
                sticky="ew",
                padx=(0, 14) if group == 0 else (0, 0),
                pady=4,
            )

        action_row = (len(self._FIELD_ROWS) + 1) // 2
        action_bar = ttk.Frame(right, style="Card.TFrame")
        action_bar.grid(
            row=action_row, column=0, columnspan=4, sticky="ew", pady=(12, 0)
        )
        for column in range(3):
            action_bar.columnconfigure(column, weight=1)
        ttk.Button(
            action_bar,
            text=self._t("pages.certificate_metrics.actions.save_draft"),
            command=self.save_draft,
        ).grid(row=0, column=0, sticky="ew", padx=(0, 6))
        ttk.Button(
            action_bar,
            text=self._t("pages.certificate_metrics.actions.submit_review"),
            style="Accent.TButton",
            command=self.submit_for_review,
        ).grid(row=0, column=1, sticky="ew", padx=6)
        ttk.Button(
            action_bar,
            text=self._t("pages.certificate_metrics.actions.reload"),
            command=self.refresh_records,
        ).grid(row=0, column=2, sticky="ew", padx=(6, 0))
        ttk.Label(
            right,
            textvariable=self.status_var,
            style="Muted.TLabel",
            wraplength=900,
            justify="left",
        ).grid(row=action_row + 1, column=0, columnspan=4, sticky="ew", pady=(8, 0))

    def refresh_records(self) -> None:
        try:
            records = self.registry.list_records()
        except Exception as exc:
            self.status_var.set(
                self._t(
                    "pages.certificate_metrics.status.load_failed",
                    message=str(exc),
                )
            )
            return
        self.records_tree.delete(*self.records_tree.get_children())
        for item in records:
            certified = item.get("certified_value")
            value = (
                "--"
                if certified is None
                else f"{certified:g} {item.get('unit') or ''}".strip()
            )
            self.records_tree.insert(
                "",
                "end",
                iid=str(item.get("record_id")),
                values=(
                    item.get("asset_name") or item.get("asset_id") or "--",
                    item.get("certificate_id") or "--",
                    value,
                    self._t(
                        f"pages.certificate_metrics.review_state.{item.get('review_state') or 'draft'}",
                        default=str(item.get("review_state") or "draft"),
                    ),
                    item.get("revision") or 1,
                ),
            )
        self.count_var.set(
            self._t(
                "pages.certificate_metrics.records.count",
                count=len(records),
            )
        )
        self.status_var.set(
            self._t(
                "pages.certificate_metrics.status.loaded",
                count=len(records),
            )
        )

    def new_record(self) -> None:
        self.current_record_id = ""
        for variable in self.form_vars.values():
            variable.set("")
        self.status_var.set(
            self._t("pages.certificate_metrics.status.new_record")
        )
        self.page_scaffold.scroll_to_top()

    def save_draft(self) -> None:
        self._save(submit_for_review=False)

    def submit_for_review(self) -> None:
        self._save(submit_for_review=True)

    def _save(self, *, submit_for_review: bool) -> None:
        values = {field: variable.get() for field, variable in self.form_vars.items()}
        values["record_id"] = self.current_record_id
        try:
            record = self.registry.save_record(
                values,
                actor="local_operator",
                submit_for_review=submit_for_review,
            )
        except ValueError as exc:
            self.status_var.set(
                self._t(
                    "pages.certificate_metrics.status.validation_failed",
                    message=str(exc),
                )
            )
            messagebox.showwarning(
                self._t("pages.certificate_metrics.validation.title"),
                str(exc),
                parent=self,
            )
            return
        except Exception as exc:
            self.status_var.set(
                self._t(
                    "pages.certificate_metrics.status.save_failed",
                    message=str(exc),
                )
            )
            messagebox.showerror(
                self._t("pages.certificate_metrics.validation.title"),
                str(exc),
                parent=self,
            )
            return
        self.current_record_id = str(record["record_id"])
        self.refresh_records()
        self._select_record(self.current_record_id)
        status_key = "submitted" if submit_for_review else "saved"
        self.status_var.set(
            self._t(
                f"pages.certificate_metrics.status.{status_key}",
                revision=record["revision"],
            )
        )

    def _on_record_selected(self, _event: tk.Event[tk.Misc]) -> None:
        selection = self.records_tree.selection()
        if not selection:
            return
        record = self.registry.get_record(selection[0])
        if record is None:
            return
        self.current_record_id = str(record.get("record_id") or "")
        for field, variable in self.form_vars.items():
            value = record.get(field)
            variable.set("" if value is None else str(value))
        self.status_var.set(
            self._t(
                "pages.certificate_metrics.status.selected",
                revision=record.get("revision") or 1,
            )
        )

    def _select_record(self, record_id: str) -> None:
        if not self.records_tree.exists(record_id):
            return
        self.records_tree.selection_set(record_id)
        self.records_tree.focus(record_id)
        self.records_tree.see(record_id)
        self._on_record_selected(None)  # type: ignore[arg-type]


__all__ = ["CertificateMetricsPage"]
