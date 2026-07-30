"""Editable offline site mapping for the V1.5 read-only initialization packet."""

from __future__ import annotations

import hashlib
import json
import re
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from typing import Any, Callable, Mapping

from ....validation.v1_5_real_acceptance_control_pack import (
    ANALYZER_BANK,
    build_v1_5_real_acceptance_site_profile_template,
    validate_v1_5_real_acceptance_site_profile,
)
from ..page_i18n import translator_for
from ..scrollable_page_frame import ScrollablePageFrame


class SiteProfilePage(ttk.Frame):
    """Edit and validate site mappings without scanning or opening COM ports."""

    def __init__(
        self,
        parent: tk.Misc,
        *,
        profile_path: str | Path,
        locale: str = "zh_CN",
        translate: Callable[..., str] | None = None,
    ) -> None:
        super().__init__(parent, style="Card.TFrame")
        self._t = translate or translator_for(locale)
        self.profile_path = Path(profile_path)
        self.profile: dict[str, Any] = {}
        self.validation: dict[str, Any] = {}
        self.last_snapshot: Mapping[str, Any] | None = None
        self.status_var = tk.StringVar(master=self, value=self._t("pages.site_profile.status.empty"))
        self.metric_vars = [tk.StringVar(master=self, value="--") for _ in range(4)]
        self.form_vars = {
            key: tk.StringVar(master=self, value="")
            for key in (
                "port",
                "ga_label",
                "protocol_device_id",
                "sn_code",
                "algorithm",
                "ftd_hz",
                "average1",
                "average2",
            )
        }
        self.bool_vars = {
            key: tk.BooleanVar(master=self, value=False)
            for key in (
                "connected",
                "powered",
                "operator_confirmed",
                "check_capable",
                "check_required",
            )
        }
        self._build()
        if self.profile_path.is_file():
            self.load_profile(self.profile_path)
        else:
            self._refresh_table()
            self._refresh_metrics()

    def render(self, snapshot: Mapping[str, Any]) -> None:
        self.last_snapshot = snapshot

    def _build(self) -> None:
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)
        scaffold = ScrollablePageFrame(self, padding=16, canvas_background="#0c1927")
        scaffold.grid(row=0, column=0, sticky="nsew")
        self.page_scaffold = scaffold
        body = scaffold.content
        body.columnconfigure(0, weight=1)

        ttk.Label(body, text=self._t("pages.site_profile.title"), style="Title.TLabel").grid(
            row=0, column=0, sticky="w"
        )
        ttk.Label(
            body,
            text=self._t("pages.site_profile.boundary"),
            style="Muted.TLabel",
            wraplength=1400,
            justify="left",
        ).grid(row=1, column=0, sticky="ew", pady=(6, 12))

        metrics = ttk.Frame(body, style="Card.TFrame")
        metrics.grid(row=2, column=0, sticky="ew", pady=(0, 12))
        metric_keys = (
            "pages.site_profile.metric.expected_connected",
            "pages.site_profile.metric.expected_powered",
            "pages.site_profile.metric.mapped",
            "pages.site_profile.metric.status",
        )
        for index, key in enumerate(metric_keys):
            metrics.columnconfigure(index, weight=1, uniform="site_metric")
            card = ttk.Frame(metrics, style="Card.TFrame", padding=10)
            card.grid(row=0, column=index, sticky="nsew", padx=4)
            ttk.Label(card, text=self._t(key), style="Muted.TLabel").pack(anchor="w")
            ttk.Label(card, textvariable=self.metric_vars[index], style="Title.TLabel").pack(
                anchor="w", pady=(5, 0)
            )

        actions = ttk.Frame(body, style="Card.TFrame")
        actions.grid(row=3, column=0, sticky="ew", pady=(0, 10))
        for index in range(5):
            actions.columnconfigure(index, weight=1)
        for index, (key, command, accent) in enumerate(
            (
                ("load_profile", self._choose_profile, False),
                ("new_from_inventory", self._choose_inventory, False),
                ("apply_row", self.apply_selected_row, False),
                ("validate", self.validate_profile, False),
                ("save", self._choose_save_path, True),
            )
        ):
            ttk.Button(
                actions,
                text=self._t(f"pages.site_profile.actions.{key}"),
                command=command,
                style="Accent.TButton" if accent else "Secondary.TButton",
            ).grid(row=0, column=index, sticky="ew", padx=4)

        table_frame = ttk.Frame(body, style="Card.TFrame")
        table_frame.grid(row=4, column=0, sticky="ew", pady=(0, 12))
        table_frame.columnconfigure(0, weight=1)
        self.tree = ttk.Treeview(
            table_frame,
            columns=("port", "visible", "connected", "powered", "ga", "protocol", "sn", "algorithm", "state"),
            show="headings",
            height=8,
        )
        self.tree.grid(row=0, column=0, sticky="ew")
        self.tree.bind("<<TreeviewSelect>>", self._on_selected)
        scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        scrollbar.grid(row=0, column=1, sticky="ns")
        self.tree.configure(yscrollcommand=scrollbar.set)
        for key, width in (
            ("port", 75),
            ("visible", 75),
            ("connected", 75),
            ("powered", 75),
            ("ga", 95),
            ("protocol", 85),
            ("sn", 100),
            ("algorithm", 130),
            ("state", 160),
        ):
            self.tree.heading(key, text=self._t(f"pages.site_profile.column.{key}"))
            self.tree.column(key, width=width, anchor="center")

        editor = ttk.LabelFrame(
            body,
            text=self._t("pages.site_profile.editor.title"),
            style="Site.TLabelframe",
            padding=12,
        )
        editor.grid(row=5, column=0, sticky="ew", pady=(0, 10))
        for column in range(8):
            editor.columnconfigure(column, weight=1)
        fields = (
            ("port", True),
            ("ga_label", False),
            ("protocol_device_id", False),
            ("sn_code", False),
            ("algorithm", False),
            ("ftd_hz", False),
            ("average1", False),
            ("average2", False),
        )
        for index, (field, readonly) in enumerate(fields):
            ttk.Label(
                editor,
                text=self._t(f"pages.site_profile.field.{field}"),
                style="Muted.TLabel",
            ).grid(row=0, column=index, sticky="w", padx=3)
            if field == "algorithm":
                widget: tk.Widget = ttk.Combobox(
                    editor,
                    textvariable=self.form_vars[field],
                    values=("legacy_ratio", "new_absorption"),
                    state="readonly",
                    style="Site.TCombobox",
                )
            else:
                widget = ttk.Entry(
                    editor,
                    textvariable=self.form_vars[field],
                    state="readonly" if readonly else "normal",
                    style="Site.TEntry",
                )
            widget.grid(row=1, column=index, sticky="ew", padx=3, pady=(3, 8))
        for index, field in enumerate(self.bool_vars):
            ttk.Checkbutton(
                editor,
                text=self._t(f"pages.site_profile.field.{field}"),
                variable=self.bool_vars[field],
                style="Site.TCheckbutton",
            ).grid(row=2, column=index, sticky="w", padx=3)

        ttk.Label(
            body,
            textvariable=self.status_var,
            style="Muted.TLabel",
            wraplength=1400,
            justify="left",
        ).grid(row=6, column=0, sticky="ew", pady=(2, 8))
        self.reason_text = tk.Text(
            body,
            height=6,
            wrap="word",
            bg="#102131",
            fg="#f2f6fa",
            relief="flat",
            padx=10,
            pady=8,
            font=("Microsoft YaHei UI", 9),
        )
        self.reason_text.grid(row=7, column=0, sticky="ew")
        self.reason_text.configure(state="disabled")

    def _rows(self) -> list[dict[str, Any]]:
        value = self.profile.get("candidate_analyzers")
        if not isinstance(value, list):
            return []
        return [row for row in value if isinstance(row, dict)]

    def _row_by_port(self, port: str) -> dict[str, Any] | None:
        return next((row for row in self._rows() if row.get("port") == port), None)

    def _refresh_table(self) -> None:
        self.tree.delete(*self.tree.get_children())
        rows = {str(row.get("port")): row for row in self._rows()}
        for port in ANALYZER_BANK:
            row = rows.get(port, {})
            state = (
                self._t("pages.site_profile.value.ready")
                if self.validation.get("ready_for_readonly_packet_build")
                and row.get("connected") is True
                else self._t("pages.site_profile.value.not_selected")
                if self.validation.get("ready_for_readonly_packet_build")
                else self._t("pages.site_profile.value.historical_prefill")
                if isinstance(row.get("identity_evidence"), Mapping)
                and row.get("operator_confirmed") is not True
                else self._t("pages.site_profile.value.review")
                if row
                else self._t("pages.site_profile.value.not_loaded")
            )
            self.tree.insert(
                "",
                "end",
                iid=port,
                values=(
                    port,
                    self._yes_no(row.get("os_visible")),
                    self._yes_no(row.get("connected")),
                    self._yes_no(row.get("powered")),
                    row.get("ga_label") or "--",
                    row.get("protocol_device_id") or "--",
                    row.get("sn_code") or "--",
                    row.get("algorithm") or "--",
                    state,
                ),
            )

    def _yes_no(self, value: Any) -> str:
        if value is True:
            return self._t("pages.site_profile.value.yes")
        if value is False:
            return self._t("pages.site_profile.value.no")
        return self._t("pages.site_profile.value.unknown")

    def _refresh_metrics(self) -> None:
        rows = self._rows()
        connected = sum(row.get("connected") is True for row in rows)
        powered = sum(row.get("powered") is True for row in rows)
        self.metric_vars[0].set(str(self.profile.get("reported_connected_count", 4)))
        self.metric_vars[1].set(str(self.profile.get("reported_powered_count", 2)))
        self.metric_vars[2].set(f"{connected} / {powered}")
        self.metric_vars[3].set(
            self._t("pages.site_profile.value.ready")
            if self.validation.get("ready_for_readonly_packet_build")
            else self._t("pages.site_profile.value.review")
        )

    def _set_reasons(self, reasons: list[str]) -> None:
        self.reason_text.configure(state="normal")
        self.reason_text.delete("1.0", "end")
        lines = (
            [self._t("pages.site_profile.reasons.none")]
            if not reasons
            else [f"• {self._display_reason(item)}" for item in reasons]
        )
        self.reason_text.insert("1.0", "\n".join(lines))
        self.reason_text.configure(state="disabled")

    def _display_reason(self, reason: str) -> str:
        exact = {
            "runtime_port_inventory_missing": "inventory_missing",
            "runtime_port_inventory_sha256_mismatch": "inventory_changed",
            "candidate_analyzer_bank_must_be_exactly_com35_to_com42": "bank_invalid",
            "reported_counts_invalid": "reported_counts_invalid",
        }
        if reason in exact:
            return self._t(f"pages.site_profile.reason.{exact[reason]}")
        match = re.fullmatch(r"(connected|powered)_count_expected_(-?\d+)_actual_(\d+)", reason)
        if match:
            return self._t(
                f"pages.site_profile.reason.{match.group(1)}_count",
                expected=match.group(2),
                actual=match.group(3),
            )
        match = re.fullmatch(r"active_powered_analyzer_count=(\d+)", reason)
        if match:
            return self._t(
                "pages.site_profile.reason.active_count",
                actual=match.group(1),
            )
        for prefix, key in (
            ("site_profile_schema=", "schema"),
            ("duplicate_ga_label=", "duplicate_ga"),
            ("duplicate_sn_code=", "duplicate_sn"),
        ):
            if reason.startswith(prefix):
                return self._t(
                    f"pages.site_profile.reason.{key}",
                    value=reason.removeprefix(prefix),
                )
        for suffix, key in (
            ("_powered_without_connected", "powered_without_connected"),
            ("_connected_not_operator_confirmed", "not_confirmed"),
            ("_connected_but_not_os_visible", "not_visible"),
            ("_connected_ga_label_missing", "ga_missing"),
            ("_protocol_device_id_missing", "protocol_missing"),
            ("_sn_code_invalid", "sn_invalid"),
            ("_algorithm_invalid", "algorithm_invalid"),
            ("_legacy_check_must_be_false", "legacy_check"),
            ("_new_algorithm_check_must_be_true", "new_check"),
            ("_runtime_1hz_evidence_missing", "runtime_1hz"),
            ("_average1_average2_evidence_missing", "average"),
        ):
            if reason.endswith(suffix):
                return self._t(
                    f"pages.site_profile.reason.{key}",
                    port=reason.removesuffix(suffix),
                )
        return self._t("pages.site_profile.reason.unknown")

    def _on_selected(self, _event: tk.Event[tk.Misc] | None = None) -> None:
        selection = self.tree.selection()
        if not selection:
            return
        row = self._row_by_port(selection[0])
        if row is None:
            return
        runtime = row.get("runtime_evidence")
        runtime = runtime if isinstance(runtime, Mapping) else {}
        for field in ("port", "ga_label", "protocol_device_id", "sn_code", "algorithm"):
            self.form_vars[field].set(str(row.get(field) or ""))
        for field in ("ftd_hz", "average1", "average2"):
            self.form_vars[field].set(str(runtime.get(field) or ""))
        for field, variable in self.bool_vars.items():
            variable.set(row.get(field) is True)

    def apply_selected_row(self) -> None:
        port = self.form_vars["port"].get()
        row = self._row_by_port(port)
        if row is None:
            self.status_var.set(self._t("pages.site_profile.status.select_row"))
            return
        for field in ("ga_label", "protocol_device_id", "sn_code", "algorithm"):
            row[field] = self.form_vars[field].get().strip()
        for field, variable in self.bool_vars.items():
            row[field] = bool(variable.get())
        runtime = row.setdefault("runtime_evidence", {})
        runtime["ftd_hz"] = self.form_vars["ftd_hz"].get().strip() or None
        runtime["average1"] = self.form_vars["average1"].get().strip()
        runtime["average2"] = self.form_vars["average2"].get().strip()
        self.validate_profile(show_dialog=False)
        self.status_var.set(self._t("pages.site_profile.status.row_applied", port=port))

    def load_profile(self, path: str | Path) -> None:
        source = Path(path)
        payload = json.loads(source.read_text(encoding="utf-8-sig"))
        if not isinstance(payload, dict):
            raise ValueError("site profile JSON must be an object")
        self.profile_path = source
        self.profile = payload
        self.validate_profile(show_dialog=False)
        prefill = payload.get("historical_identity_prefill")
        prefill = prefill if isinstance(prefill, Mapping) else {}
        if int(prefill.get("applied_count") or 0) > 0:
            self.status_var.set(
                self._t(
                    "pages.site_profile.status.loaded_historical",
                    path=source.name,
                    count=prefill["applied_count"],
                )
            )
        else:
            self.status_var.set(
                self._t("pages.site_profile.status.loaded", path=source.name)
            )

    def create_from_inventory(self, path: str | Path) -> None:
        self.profile = build_v1_5_real_acceptance_site_profile_template(
            runtime_port_inventory_json=path,
            reported_connected_count=4,
            reported_powered_count=2,
            observation_id="operator_site_mapping_draft",
        )
        self.validation = {}
        self._refresh_table()
        self._refresh_metrics()
        self._set_reasons(["operator_mapping_required"])
        self.status_var.set(self._t("pages.site_profile.status.template_created"))

    def validate_profile(self, *, show_dialog: bool = True) -> dict[str, Any]:
        inventory_path = str(self.profile.get("runtime_port_inventory_json") or "")
        if not self.profile or not inventory_path or not Path(inventory_path).is_file():
            self.validation = {
                "status": "review_required",
                "ready_for_readonly_packet_build": False,
                "reasons": ["runtime_port_inventory_missing"],
                "reviewed_port_inventory": {"reviewed_ports": []},
                "active_analyzer_list": {"active_analyzers": []},
            }
        else:
            self.validation = validate_v1_5_real_acceptance_site_profile(
                site_profile=self.profile,
                runtime_port_inventory_json=inventory_path,
            )
        reasons = [str(item) for item in self.validation.get("reasons") or []]
        self._refresh_table()
        self._refresh_metrics()
        self._set_reasons(reasons)
        self.status_var.set(
            self._t("pages.site_profile.status.valid")
            if not reasons
            else self._t("pages.site_profile.status.invalid", count=len(reasons))
        )
        if show_dialog:
            messagebox.showinfo(
                self._t("pages.site_profile.dialog.validation"),
                self.status_var.get(),
                parent=self,
            )
        return self.validation

    def save_profile(self, path: str | Path | None = None) -> dict[str, Path]:
        if not self.profile:
            raise ValueError("site profile is not loaded")
        target = Path(path) if path else self.profile_path
        if not str(target):
            raise ValueError("site profile output path is required")
        validation = self.validate_profile(show_dialog=False)
        self.profile["profile_status"] = validation["status"]
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(self.profile, ensure_ascii=False, indent=2), encoding="utf-8-sig")
        profile_sha = hashlib.sha256(target.read_bytes()).hexdigest()
        blocked = list(validation.get("reasons") or [])
        reviewed = dict(validation.get("reviewed_port_inventory") or {})
        active = dict(validation.get("active_analyzer_list") or {})
        for payload in (reviewed, active):
            payload["source_site_profile_json"] = str(target.resolve())
            payload["source_site_profile_sha256"] = profile_sha
            payload["blocked_reasons"] = blocked
        if blocked:
            reviewed["reviewed_ports"] = []
            active["active_analyzers"] = []
        validation_path = target.with_name("v1_5_real_acceptance_site_profile_validation.json")
        reviewed_path = target.with_name("v1_5_readonly_com_reviewed_port_inventory.json")
        active_path = target.with_name("v1_5_readonly_com_active_analyzer_list.json")
        validation_path.write_text(
            json.dumps(validation, ensure_ascii=False, indent=2), encoding="utf-8-sig"
        )
        reviewed_path.write_text(json.dumps(reviewed, ensure_ascii=False, indent=2), encoding="utf-8-sig")
        active_path.write_text(json.dumps(active, ensure_ascii=False, indent=2), encoding="utf-8-sig")
        self.profile_path = target
        self.status_var.set(
            self._t(
                "pages.site_profile.status.saved_ready"
                if not blocked
                else "pages.site_profile.status.saved_blocked",
                path=target.name,
            )
        )
        return {
            "profile": target,
            "validation": validation_path,
            "reviewed_ports": reviewed_path,
            "active_analyzers": active_path,
        }

    def _choose_profile(self) -> None:
        selected = filedialog.askopenfilename(
            parent=self,
            filetypes=((self._t("pages.site_profile.dialog.json"), "*.json"),),
        )
        if selected:
            try:
                self.load_profile(selected)
            except Exception as exc:
                messagebox.showerror(
                    self._t("pages.site_profile.dialog.load_failed"), str(exc), parent=self
                )

    def _choose_inventory(self) -> None:
        selected = filedialog.askopenfilename(
            parent=self,
            filetypes=((self._t("pages.site_profile.dialog.json"), "*.json"),),
        )
        if selected:
            try:
                self.create_from_inventory(selected)
            except Exception as exc:
                messagebox.showerror(
                    self._t("pages.site_profile.dialog.load_failed"), str(exc), parent=self
                )

    def _choose_save_path(self) -> None:
        selected = filedialog.asksaveasfilename(
            parent=self,
            initialfile=self.profile_path.name or "v1_5_real_acceptance_site_profile.json",
            defaultextension=".json",
            filetypes=((self._t("pages.site_profile.dialog.json"), "*.json"),),
        )
        if not selected:
            return
        try:
            self.save_profile(selected)
        except Exception as exc:
            messagebox.showerror(
                self._t("pages.site_profile.dialog.save_failed"), str(exc), parent=self
            )


__all__ = ["SiteProfilePage"]
