"""One declarative, read-only implementation for V1.5 summary pages."""

from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Any, Callable, Mapping

from ..page_i18n import translator_for
from ..scrollable_page_frame import ScrollablePageFrame


_PAGE_KINDS = {
    "algorithm",
    "devices",
    "plan",
    "qc",
    "results",
    "reports",
    "review",
}


class ReadOnlySummaryPage(ttk.Frame):
    """Render one section of the shared V1.5 snapshot without control actions."""

    def __init__(
        self,
        parent: tk.Misc,
        *,
        page_kind: str,
        locale: str = "zh_CN",
        translate: Callable[..., str] | None = None,
    ) -> None:
        if page_kind not in _PAGE_KINDS:
            raise ValueError(f"unsupported read-only page kind: {page_kind}")
        super().__init__(parent, style="Card.TFrame")
        self.page_kind = page_kind
        self._t = translate or translator_for(locale)
        self.last_snapshot: Mapping[str, Any] | None = None
        self.metric_vars = [
            tk.StringVar(master=self, value="--") for _ in range(4)
        ]
        self.readonly_widgets: list[tk.Text] = []
        self._build()

    def _build(self) -> None:
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)
        scaffold = ScrollablePageFrame(
            self,
            padding=18,
            canvas_background="#0c1927",
        )
        scaffold.grid(row=0, column=0, sticky="nsew")
        self.page_scaffold = scaffold
        body = scaffold.content
        body.columnconfigure(0, weight=1)

        ttk.Label(
            body,
            text=self._t(f"pages.readonly.{self.page_kind}.title"),
            style="Title.TLabel",
        ).grid(row=0, column=0, sticky="w")
        ttk.Label(
            body,
            text=self._t(f"pages.readonly.{self.page_kind}.boundary"),
            style="Muted.TLabel",
            wraplength=1400,
            justify="left",
        ).grid(row=1, column=0, sticky="ew", pady=(6, 14))

        metrics = ttk.Frame(body, style="Card.TFrame")
        metrics.grid(row=2, column=0, sticky="ew", pady=(0, 14))
        metric_keys = self._metric_keys()
        for index, key in enumerate(metric_keys):
            metrics.columnconfigure(index, weight=1, uniform="summary_metric")
            card = ttk.Frame(metrics, style="Card.TFrame", padding=12)
            card.grid(row=0, column=index, sticky="nsew", padx=4)
            ttk.Label(
                card,
                text=self._t(key),
                style="Muted.TLabel",
            ).pack(anchor="w")
            ttk.Label(
                card,
                textvariable=self.metric_vars[index],
                style="Title.TLabel",
            ).pack(anchor="w", pady=(6, 0))

        section_grid = ttk.Frame(body, style="Card.TFrame")
        section_grid.grid(row=3, column=0, sticky="nsew")
        section_grid.columnconfigure(0, weight=1)
        section_grid.columnconfigure(1, weight=1)
        for index, key in enumerate(self._section_keys()):
            widget = self._text_section(
                section_grid,
                row=index // 2,
                column=index % 2,
                title=self._t(key),
            )
            if self.page_kind == "algorithm":
                widget.configure(height=11)
            self.readonly_widgets.append(widget)

    def _metric_keys(self) -> tuple[str, ...]:
        return {
            "results": (
                "pages.readonly.metric.status",
                "pages.readonly.metric.co2_points",
                "pages.readonly.metric.h2o_points",
                "pages.readonly.metric.routes",
            ),
            "reports": (
                "pages.readonly.metric.status",
                "pages.readonly.metric.artifacts",
                "pages.readonly.metric.present",
                "pages.readonly.metric.roles",
            ),
            "review": (
                "pages.readonly.metric.review_status",
                "pages.readonly.metric.safety",
                "pages.readonly.metric.certificates",
                "pages.readonly.metric.release",
            ),
            "plan": (
                "pages.readonly.metric.status",
                "pages.readonly.metric.profile",
                "pages.readonly.metric.total_points",
                "pages.readonly.metric.routes",
            ),
            "qc": (
                "pages.readonly.metric.qc_status",
                "pages.readonly.metric.point_contract",
                "pages.readonly.metric.route_closure",
                "pages.readonly.metric.real_samples",
            ),
            "devices": (
                "pages.readonly.metric.device_status",
                "pages.readonly.metric.configured_channels",
                "pages.readonly.metric.connected_channels",
                "pages.readonly.metric.unknown_health",
            ),
            "algorithm": (
                "pages.readonly.metric.algorithm_status",
                "pages.readonly.metric.production_profile",
                "pages.readonly.metric.algorithm_mode",
                "pages.readonly.metric.shadow_candidates",
            ),
        }[self.page_kind]

    def _section_keys(self) -> tuple[str, ...]:
        return {
            "results": (
                "pages.readonly.section.routes",
                "pages.readonly.section.anchors",
                "pages.readonly.section.warnings",
                "pages.readonly.section.blockers",
            ),
            "reports": (
                "pages.readonly.section.artifacts",
                "pages.readonly.section.roles",
                "pages.readonly.section.evidence",
                "pages.readonly.section.report_boundary",
            ),
            "review": (
                "pages.readonly.section.safety",
                "pages.readonly.section.certificate",
                "pages.readonly.section.release",
                "pages.readonly.section.next_actions",
            ),
            "plan": (
                "pages.readonly.section.plan_routes",
                "pages.readonly.section.plan_boundary",
                "pages.readonly.section.plan_certificate",
                "pages.readonly.section.warnings",
            ),
            "qc": (
                "pages.readonly.section.qc_checks",
                "pages.readonly.section.qc_point_evidence",
                "pages.readonly.section.qc_rule_governance",
                "pages.readonly.section.qc_boundary",
            ),
            "devices": (
                "pages.readonly.section.device_channels",
                "pages.readonly.section.device_identity",
                "pages.readonly.section.device_health",
                "pages.readonly.section.device_boundary",
            ),
            "algorithm": (
                "pages.readonly.section.production_profile",
                "pages.readonly.section.shadow_candidates",
                "pages.readonly.section.physical_contract",
                "pages.readonly.section.algorithm_boundary",
            ),
        }[self.page_kind]

    @staticmethod
    def _text_section(
        parent: tk.Misc,
        *,
        row: int,
        column: int,
        title: str,
    ) -> tk.Text:
        panel = ttk.Frame(parent, style="Card.TFrame", padding=12)
        panel.grid(
            row=row,
            column=column,
            sticky="nsew",
            padx=(0, 6) if column == 0 else (6, 0),
            pady=(0, 12),
        )
        panel.columnconfigure(0, weight=1)
        ttk.Label(panel, text=title, style="Muted.TLabel").grid(
            row=0,
            column=0,
            sticky="w",
            pady=(0, 6),
        )
        widget = tk.Text(
            panel,
            height=9,
            wrap="word",
            bg="#102131",
            fg="#f2f6fa",
            insertbackground="#f2f6fa",
            selectbackground="#153b63",
            relief="flat",
            borderwidth=0,
            padx=10,
            pady=9,
            font=("Microsoft YaHei UI", 9),
        )
        widget.grid(row=1, column=0, sticky="nsew")
        widget.configure(state="disabled")
        return widget

    @staticmethod
    def _set_text(widget: tk.Text, lines: list[str]) -> None:
        widget.configure(state="normal")
        widget.delete("1.0", "end")
        widget.insert("1.0", "\n".join(lines).strip() + "\n")
        widget.configure(state="disabled")

    def render(self, snapshot: Mapping[str, Any]) -> None:
        self.last_snapshot = snapshot
        if self.page_kind == "results":
            metrics, sections = self._render_results(snapshot)
        elif self.page_kind == "reports":
            metrics, sections = self._render_reports(snapshot)
        elif self.page_kind == "plan":
            metrics, sections = self._render_plan(snapshot)
        elif self.page_kind == "qc":
            metrics, sections = self._render_qc(snapshot)
        elif self.page_kind == "devices":
            metrics, sections = self._render_devices(snapshot)
        elif self.page_kind == "algorithm":
            metrics, sections = self._render_algorithm(snapshot)
        else:
            metrics, sections = self._render_review(snapshot)
        for variable, value in zip(self.metric_vars, metrics, strict=True):
            variable.set(value)
        for widget, lines in zip(self.readonly_widgets, sections, strict=True):
            self._set_text(widget, lines)
        self.page_scaffold._update_scroll_region()

    def _render_results(
        self,
        snapshot: Mapping[str, Any],
    ) -> tuple[list[str], list[list[str]]]:
        results = dict(snapshot.get("results") or {})
        counts = dict(results.get("point_counts") or {})
        routes = [dict(item) for item in results.get("route_results") or ()]
        route_lines = [
            self._t(
                "pages.readonly.value.route",
                route=str(row.get("route_kind") or "--").upper(),
                status=str(row.get("status") or "pending"),
                count=int(row.get("point_count") or 0),
            )
            for row in routes
        ] or [self._t("pages.readonly.value.not_started")]
        anchors = [
            self._t("pages.readonly.value.co2_anchor"),
            self._t("pages.readonly.value.h2o_anchor"),
        ]
        warnings = [
            f"• {item}" for item in results.get("warnings") or ()
        ] or [self._t("pages.readonly.value.none")]
        blockers = [
            f"• {item}" for item in results.get("blockers") or ()
        ] or [self._t("pages.readonly.value.none")]
        return (
            [
                str(results.get("status") or "not_started"),
                str(int(counts.get("co2", 45) or 45)),
                str(int(counts.get("h2o", 13) or 13)),
                str(len(routes)),
            ],
            [route_lines, anchors, warnings, blockers],
        )

    def _render_reports(
        self,
        snapshot: Mapping[str, Any],
    ) -> tuple[list[str], list[list[str]]]:
        reports = dict(snapshot.get("reports") or {})
        artifacts = [dict(item) for item in reports.get("artifacts") or ()]
        artifact_lines = [
            self._t(
                "pages.readonly.value.artifact",
                name=str(item.get("display_name") or "--"),
                role=str(item.get("role") or "--"),
                status=str(item.get("export_status") or "missing"),
            )
            for item in artifacts
        ] or [self._t("pages.readonly.value.none")]
        roles = [
            f"• {role}" for role in reports.get("allowed_roles") or ()
        ]
        roles.append(
            self._t(
                "pages.readonly.value.report_statuses",
                statuses=" / ".join(
                    str(item)
                    for item in reports.get("allowed_export_statuses") or ()
                )
                or "--",
            )
        )
        evidence_lines = [
            self._t(
                "pages.readonly.value.report_authority",
                authority=str(reports.get("authority") or "--"),
            ),
            self._t("pages.readonly.value.simulated_evidence"),
            self._t("pages.readonly.value.no_paths"),
        ]
        boundary_lines = [
            self._t("pages.readonly.value.report_read_only"),
            self._t(
                "pages.readonly.value.report_release",
                status=str(
                    reports.get("formal_release_status")
                    or "not_evaluated"
                ),
            ),
            self._t("pages.readonly.value.not_acceptance"),
        ]
        return (
            [
                str(snapshot.get("overall_status") or "not_started"),
                str(int(reports.get("artifact_count") or 0)),
                str(int(reports.get("present_count") or 0)),
                str(len(reports.get("allowed_roles") or ())),
            ],
            [
                artifact_lines,
                roles,
                evidence_lines,
                boundary_lines,
            ],
        )

    def _render_review(
        self,
        snapshot: Mapping[str, Any],
    ) -> tuple[list[str], list[list[str]]]:
        review = dict(snapshot.get("review") or {})
        safety = dict(snapshot.get("safety") or {})
        certificate = dict(snapshot.get("certificate") or {})
        violations = list(safety.get("violations") or ())
        safety_lines = [
            self._t(
                "pages.readonly.value.safety",
                status=str(safety.get("status") or "pending"),
            ),
            self._t("pages.readonly.value.no_device_actions"),
        ]
        if violations:
            safety_lines.extend(f"• {item}" for item in violations)
        cert_lines = [
            self._t(
                "pages.readonly.value.certificate",
                count=int(certificate.get("record_count") or 0),
                gate=str(certificate.get("start_gate") or "non_blocking"),
            ),
            self._t("pages.readonly.value.certificate_isolated"),
        ]
        release_lines = [
            self._t("pages.readonly.value.not_acceptance"),
            self._t("pages.readonly.value.no_approval"),
        ]
        actions = [
            f"• {item}" for item in review.get("next_actions") or ()
        ] or [self._t("pages.readonly.value.none")]
        return (
            [
                str(review.get("overall_status") or "pending"),
                str(safety.get("status") or "pending"),
                str(int(certificate.get("record_count") or 0)),
                self._t("pages.readonly.value.not_released"),
            ],
            [safety_lines, cert_lines, release_lines, actions],
        )

    def _render_plan(
        self,
        snapshot: Mapping[str, Any],
    ) -> tuple[list[str], list[list[str]]]:
        plan = dict(snapshot.get("plan") or {})
        routes = [dict(item) for item in plan.get("routes") or ()]
        route_lines = [
            self._t(
                "pages.readonly.value.plan_route",
                route=str(row.get("route_kind") or "--").upper(),
                status=str(row.get("status") or "planned"),
                count=int(row.get("point_count") or 0),
                mode=str(row.get("execution_mode") or "mature_runner_dry_run"),
            )
            for row in routes
        ] or [self._t("pages.readonly.value.none")]
        boundary_lines = [
            self._t("pages.readonly.value.plan_read_only"),
            self._t("pages.readonly.value.plan_dry_run_only"),
            self._t("pages.readonly.value.no_device_actions"),
        ]
        certificate_lines = [
            self._t(
                "pages.readonly.value.plan_certificate",
                gate=str(plan.get("certificate_start_gate") or "non_blocking"),
            ),
            self._t("pages.readonly.value.certificate_isolated"),
        ]
        warnings = [
            f"• {item}" for item in plan.get("warnings") or ()
        ] or [self._t("pages.readonly.value.none")]
        return (
            [
                str(plan.get("status") or "planned"),
                str(plan.get("profile_id") or "legacy_ratio_production"),
                str(int(plan.get("total_points") or 0)),
                str(len(routes)),
            ],
            [route_lines, boundary_lines, certificate_lines, warnings],
        )

    def _render_qc(
        self,
        snapshot: Mapping[str, Any],
    ) -> tuple[list[str], list[list[str]]]:
        qc = dict(snapshot.get("qc") or {})
        checks = [dict(item) for item in qc.get("checks") or ()]
        check_statuses = {
            str(row.get("check_id") or ""): str(row.get("status") or "pending")
            for row in checks
        }
        check_lines = [
            self._t(
                "pages.readonly.value.qc_check",
                check=str(row.get("check_id") or "--"),
                status=str(row.get("status") or "pending"),
            )
            for row in checks
        ] or [self._t("pages.readonly.value.none")]
        warnings = [
            f"• {item}" for item in qc.get("warnings") or ()
        ] or [self._t("pages.readonly.value.none")]
        blockers = [
            f"• {item}" for item in qc.get("blockers") or ()
        ] or [self._t("pages.readonly.value.none")]
        point_evidence = dict(qc.get("point_evidence_contract") or {})
        point_evidence_lines = [
            self._t(
                "pages.readonly.value.qc_point_status",
                status=str(point_evidence.get("status") or "not_evaluated"),
                authority=str(point_evidence.get("authority") or "--"),
            ),
            self._t(
                "pages.readonly.value.qc_point_roles",
                roles=", ".join(
                    str(item)
                    for item in point_evidence.get("artifact_roles") or ()
                )
                or "--",
            ),
            self._t(
                "pages.readonly.value.qc_point_fields",
                fields=", ".join(
                    str(item)
                    for item in point_evidence.get("required_fields") or ()
                )
                or "--",
            ),
        ]
        governance = dict(qc.get("rule_threshold_governance") or {})
        reject_summary = dict(qc.get("reject_reason_summary") or {})
        governance_lines = [
            self._t(
                "pages.readonly.value.qc_rule_status",
                status=str(governance.get("status") or "--"),
                source=str(governance.get("source") or "--"),
            ),
            self._t(
                "pages.readonly.value.qc_rule_ui_edit",
                allowed=str(
                    bool(governance.get("ui_edit_allowed", False))
                ).lower(),
            ),
            self._t(
                "pages.readonly.value.qc_reject_status",
                status=str(
                    reject_summary.get("status") or "not_evaluated"
                ),
                role=str(
                    reject_summary.get("source_artifact_role")
                    or "execution_summary"
                ),
            ),
        ]
        boundary_lines = [
            self._t("pages.readonly.value.qc_dry_run_only"),
            self._t("pages.readonly.value.qc_not_evaluated"),
            self._t("pages.readonly.value.not_acceptance"),
            self._t("pages.readonly.section.warnings"),
            *warnings,
            self._t("pages.readonly.section.blockers"),
            *blockers,
        ]
        return (
            [
                str(qc.get("overall_status") or "pending"),
                check_statuses.get("point_count_contract", "pending"),
                check_statuses.get("route_dry_run_closure", "pending"),
                check_statuses.get("sample_stability", "not_evaluated"),
            ],
            [
                check_lines,
                point_evidence_lines,
                governance_lines,
                boundary_lines,
            ],
        )

    def _render_devices(
        self,
        snapshot: Mapping[str, Any],
    ) -> tuple[list[str], list[list[str]]]:
        devices = dict(snapshot.get("devices") or {})
        channels = [dict(item) for item in devices.get("channels") or ()]
        channel_lines = [
            self._t(
                "pages.readonly.value.device_channel",
                channel=str(row.get("display_name") or "--"),
                connection=str(row.get("connection_status") or "not_connected"),
                identity=str(row.get("identity_status") or "not_evaluated"),
                health=str(row.get("health_status") or "not_evaluated"),
            )
            for row in channels
        ] or [self._t("pages.readonly.value.none")]
        identity_lines = [
            self._t(
                "pages.readonly.value.device_mapping_counts",
                configured=int(devices.get("configured_channel_count") or 0),
                reported=int(devices.get("reported_connected_count") or 0),
                mapped=int(devices.get("mapped_connected_count") or 0),
                powered=int(devices.get("powered_count") or 0),
            ),
            self._t(
                "pages.readonly.value.device_identity_count",
                count=int(devices.get("identity_evaluated_count") or 0),
            ),
        ]
        freshness = dict(devices.get("runtime_freshness") or {})
        health_lines = [
            self._t(
                "pages.readonly.value.device_runtime_freshness",
                status=str(freshness.get("status") or "unknown"),
                age=(
                    str(freshness.get("age_seconds"))
                    if freshness.get("age_seconds") is not None
                    else "--"
                ),
            ),
            self._t(
                "pages.readonly.value.device_health_count",
                count=int(devices.get("health_evaluated_count") or 0),
                unknown=int(devices.get("unknown_health_count") or 0),
            ),
        ]
        initialization = dict(
            devices.get("initialization_contract") or {}
        )
        boundary_lines = [
            self._t(
                "pages.readonly.value.device_read_only_mode",
                mode=str(devices.get("ui_mode") or "read_only"),
                observed=int(devices.get("connected_count") or 0),
                mapped=int(devices.get("mapped_connected_count") or 0),
                powered=int(devices.get("powered_count") or 0),
            ),
            self._t(
                "pages.readonly.value.device_runtime_authority",
                authority=str(
                    devices.get("runtime_state_authority")
                    or "mature_v1_5_runner_only"
                ),
            ),
            self._t(
                "pages.readonly.value.device_initialization_contract",
                owner=str(
                    initialization.get("owner")
                    or "mature_v1_5_initialization_flow"
                ),
                mode=str(initialization.get("runtime_mode") or "MODE2"),
                rate=str(initialization.get("upload_rate_hz") or 1),
                rate_scope=str(
                    initialization.get("upload_rate_scope")
                    or "calibration_upload_timebase"
                ),
                average1=str(initialization.get("average1") or "--"),
                average2=str(initialization.get("average2") or "--"),
                temperature=str(
                    initialization.get("temperature_coefficients")
                    or "SENCO7_SENCO8_neutral"
                ),
            ),
            self._t("pages.readonly.value.device_no_v2_workbench"),
            self._t("pages.readonly.value.no_device_actions"),
            self._t("pages.readonly.value.not_acceptance"),
        ]
        return (
            [
                str(devices.get("overall_status") or "simulation_only"),
                str(int(devices.get("configured_channel_count") or 0)),
                str(int(devices.get("connected_count") or 0)),
                str(int(devices.get("unknown_health_count") or 0)),
            ],
            [channel_lines, identity_lines, health_lines, boundary_lines],
        )

    def _render_algorithm(
        self,
        snapshot: Mapping[str, Any],
    ) -> tuple[list[str], list[list[str]]]:
        algorithm = dict(snapshot.get("algorithm") or {})
        production = dict(algorithm.get("production_profile") or {})
        candidates = [
            dict(item) for item in algorithm.get("shadow_candidates") or ()
        ]
        physical = dict(algorithm.get("physical_contract") or {})
        production_lines = [
            self._t(
                "pages.readonly.value.production_profile",
                profile=str(production.get("profile_id") or "--"),
                mode=str(production.get("algorithm_mode") or "--"),
                review=str(production.get("review_status") or "not_evaluated"),
            ),
            self._t("pages.readonly.value.production_45_13"),
        ]
        candidate_lines = [
            self._t(
                "pages.readonly.value.shadow_candidate",
                profile=str(row.get("profile_id") or "--"),
                mode=str(row.get("algorithm_mode") or "--"),
                state=str(row.get("promotion_state") or "blocked"),
            )
            for row in candidates
        ] or [self._t("pages.readonly.value.none")]
        physical_lines = [
            self._t(
                "pages.readonly.value.pressure_contract",
                value=str(physical.get("pressure_sequence") or "--"),
            ),
            self._t("pages.readonly.value.pressure_truth_contract"),
            self._t("pages.readonly.value.pressure_fit_contract"),
            self._t(
                "pages.readonly.value.temperature_contract",
                value=str(physical.get("temperature_coefficients") or "--"),
            ),
            self._t("pages.readonly.value.temperature_truth_contract"),
            self._t("pages.readonly.value.flow_source_contract"),
            self._t("pages.readonly.value.sampling_timebase_contract"),
            self._t("pages.readonly.value.average_contract"),
            self._t("pages.readonly.value.co2_anchor"),
            self._t("pages.readonly.value.h2o_anchor"),
        ]
        boundary_lines = [
            self._t("pages.readonly.value.algorithm_no_auto_select"),
            self._t("pages.readonly.value.algorithm_no_switch"),
            self._t("pages.readonly.value.not_acceptance"),
        ]
        return (
            [
                str(algorithm.get("overall_status") or "blocked"),
                str(production.get("profile_id") or "--"),
                str(production.get("algorithm_mode") or "--"),
                str(len(candidates)),
            ],
            [
                production_lines,
                candidate_lines,
                physical_lines,
                boundary_lines,
            ],
        )


__all__ = ["ReadOnlySummaryPage"]
