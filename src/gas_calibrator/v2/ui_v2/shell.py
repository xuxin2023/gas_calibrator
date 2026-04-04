from __future__ import annotations

import tkinter as tk
from tkinter import messagebox
from tkinter import ttk
from typing import Any

from .controllers.shortcut_manager import ShortcutManager
from .dialogs.about_dialog import AboutDialog
from .dialogs.licenses_dialog import LicensesDialog
from .dialogs.preferences_dialog import PreferencesDialog
from .dialogs.release_notes_dialog import ReleaseNotesDialog
from .diagnostics.diagnostic_bundle_exporter import DiagnosticBundleExporter
from .controllers.live_state_feed import LiveStateFeed
from .i18n import t
from .controllers.run_controller import RunController
from .pages.algorithms_page import AlgorithmsPage
from .pages.devices_page import DevicesPage
from .pages.plan_editor_page import PlanEditorPage
from .pages.qc_page import QCPage
from .pages.reports_page import ReportsPage
from .pages.results_page import ResultsPage
from .pages.run_control_page import RunControlPage
from .styles import THEME, apply_styles
from .widgets.busy_overlay import BusyOverlay
from .widgets.error_banner import ErrorBanner
from .widgets.log_panel import LogPanel
from .widgets.metric_card import MetricCard
from .widgets.notification_center import NotificationCenter
from .runtime.crash_recovery import CrashRecovery
from .utils.route_memory import RouteMemory
from .utils.screenshot import export_widget_screenshot


class AppShell:
    """Main Tk shell for the V2 cockpit."""

    def __init__(
        self,
        root: tk.Tk,
        *,
        facade: Any,
        run_controller: RunController,
        live_state_feed: LiveStateFeed,
        route_memory: RouteMemory | None = None,
        crash_recovery: CrashRecovery | None = None,
        app_info: dict[str, Any] | None = None,
        release_notes_text: str = "",
        licenses_text: str = "",
    ) -> None:
        self.root = root
        self.facade = facade
        self.run_controller = run_controller
        self.live_state_feed = live_state_feed
        self.route_memory = route_memory
        self.crash_recovery = crash_recovery
        self.app_info = dict(app_info or self.facade.get_app_info())
        self.release_notes_text = str(release_notes_text or "")
        self.licenses_text = str(licenses_text or t("shell.license_fallback"))
        self.theme = apply_styles(root)
        self.diagnostic_exporter = DiagnosticBundleExporter(self.facade.runtime_paths)

        self.run_id_var = tk.StringVar(value="--")
        self.phase_var = tk.StringVar(value="--")
        self.point_var = tk.StringVar(value="--")
        self.progress_var = tk.StringVar(value="0.0%")
        self.route_var = tk.StringVar(value="--")
        self.message_var = tk.StringVar(value=t("common.ready"))
        self._last_logs: list[str] = []
        self._pages: dict[str, ttk.Frame] = {}
        self.current_page_name = "run"
        self.preferences_dialog: PreferencesDialog | None = None
        self.about_dialog: AboutDialog | None = None
        self.licenses_dialog: LicensesDialog | None = None
        self.release_notes_dialog: ReleaseNotesDialog | None = None
        self._last_recovery_signature: tuple[str, ...] | None = None
        self._recovery_prompt_pending = bool(self.crash_recovery is not None and self.crash_recovery.has_pending_recovery())
        self._restoring_workspace_split = False
        self._shell_shutdown = False
        self._root_destroyed = False
        self._display_profile_sync_idle_after_id: str | None = None
        self._display_profile_sync_after_id: str | None = None
        self._last_display_profile_metrics: tuple[int, int, int, int] | None = None

        self._build()
        self.shortcut_manager = ShortcutManager(
            root,
            shell=self,
            run_controller=self.run_controller,
            open_preferences=self.open_preferences_dialog,
            open_about=self.open_about_dialog,
        )
        self.shortcut_manager.bind_default_shortcuts()
        self.live_state_feed.bind_root(root)
        self.live_state_feed.set_sink(self.apply_snapshot)
        self.live_state_feed.set_devices_sink(self._pages["devices"].render)
        self.live_state_feed.set_algorithms_sink(self._pages["algorithms"].render)
        self.live_state_feed.set_reports_sink(self._pages["reports"].render)
        self.live_state_feed.set_error_sink(self.error_banner.render)
        self.live_state_feed.set_busy_sink(self.busy_overlay.render)
        self.live_state_feed.set_notification_sink(self.notification_center.render)
        self.apply_snapshot(self.facade.build_snapshot())
        remembered = self.route_memory.load() if self.route_memory is not None else "run"
        if remembered in self._pages:
            self.show_page(remembered)
        self._prompt_crash_recovery_if_needed()

    def start(self) -> None:
        self.live_state_feed.start()

    def shutdown(self) -> None:
        if self._shell_shutdown:
            return
        self._shell_shutdown = True
        self._cancel_display_profile_context_sync_callbacks()
        self._remember_workspace_split()
        self.live_state_feed.stop()
        if self.crash_recovery is not None:
            self.crash_recovery.clear()
        if hasattr(self.facade, "shutdown"):
            self.facade.shutdown()

    def show_page(self, name: str) -> None:
        self.current_page_name = name
        self._pages[name].tkraise()
        if self.route_memory is not None:
            self.route_memory.save(name)
        self._refresh_recovery_snapshot()

    def open_preferences_dialog(self) -> None:
        self.preferences_dialog = PreferencesDialog(
            self.root,
            initial=self.facade.get_preferences(),
            on_save=self.facade.save_preferences,
        )

    def open_about_dialog(self) -> None:
        self.about_dialog = AboutDialog(self.root, app_info=self.app_info)

    def open_release_notes_dialog(self) -> None:
        self.release_notes_dialog = ReleaseNotesDialog(self.root, notes_text=self.release_notes_text)

    def open_licenses_dialog(self) -> None:
        self.licenses_dialog = LicensesDialog(self.root, licenses_text=self.licenses_text)

    def export_diagnostic_bundle(self) -> dict[str, Any]:
        result = self.diagnostic_exporter.export(self.facade)
        if bool(result.get("ok", False)):
            self.facade.log_ui(t("shell.log.diagnostic_bundle_exported", path=result["bundle_dir"]))
        else:
            self.facade.log_ui(
                t("shell.log.diagnostic_bundle_export_failed", message=result.get("message", t("common.none")))
            )
        return result

    def apply_snapshot(self, snapshot: dict[str, Any]) -> None:
        run = dict(snapshot.get("run", {}) or {})
        self.run_id_var.set(str(run.get("run_id", "--") or "--"))
        self.phase_var.set(str(run.get("phase_display", run.get("phase", "--")) or "--"))
        self.point_var.set(str(run.get("current_point", "--") or "--"))
        self.progress_var.set(f"{float(run.get('progress_pct', 0.0) or 0.0):.1f}%")
        self.route_var.set(str(run.get("route_display", run.get("route", "--")) or "--"))
        self.message_var.set(str(run.get("message_display", run.get("message", "--")) or "--"))

        self._pages["run"].render(snapshot.get("run", {}))
        self._pages["qc"].render(snapshot.get("qc", {}))
        self._pages["results"].render(snapshot.get("results", {}))
        self._pages["devices"].render(snapshot.get("devices", {}))
        self._pages["algorithms"].render(snapshot.get("algorithms", {}))
        self._pages["reports"].render(snapshot.get("reports", {}))
        self.error_banner.render(snapshot.get("error", {}))
        self.busy_overlay.render(snapshot.get("busy", {}))
        self.notification_center.render(snapshot.get("notifications", {}))
        self._render_logs(list(snapshot.get("logs", []) or []))
        self._refresh_recovery_snapshot(snapshot)

    def _build(self) -> None:
        self.root.title(t("shell.title"))
        self.root.minsize(1360, 820)
        self.root.configure(bg=THEME.bg)
        self.root.configure(menu=self._build_menu())
        self._apply_initial_geometry()

        self.root.rowconfigure(1, weight=1)
        self.root.columnconfigure(1, weight=1)
        self.root.grid_columnconfigure(0, minsize=216)

        top = ttk.Frame(self.root, style="Card.TFrame", padding=8)
        top.grid(row=0, column=0, columnspan=2, sticky="ew", padx=8, pady=(8, 4))
        for column in range(6):
            top.columnconfigure(column, weight=1)
        self.top_metrics = top
        MetricCard(top, title=t("shell.metric.run_id"), value_var=self.run_id_var).grid(row=0, column=0, sticky="nsew", padx=4)
        MetricCard(top, title=t("shell.metric.phase"), value_var=self.phase_var).grid(row=0, column=1, sticky="nsew", padx=4)
        MetricCard(top, title=t("shell.metric.point"), value_var=self.point_var).grid(row=0, column=2, sticky="nsew", padx=4)
        MetricCard(top, title=t("shell.metric.progress"), value_var=self.progress_var).grid(row=0, column=3, sticky="nsew", padx=4)
        MetricCard(top, title=t("shell.metric.route"), value_var=self.route_var).grid(row=0, column=4, sticky="nsew", padx=4)
        MetricCard(top, title=t("shell.metric.message"), value_var=self.message_var).grid(row=0, column=5, sticky="nsew", padx=4)
        self.error_banner = ErrorBanner(top)
        self.error_banner.grid(row=1, column=0, columnspan=6, sticky="ew", padx=4, pady=(8, 0))

        nav = ttk.Frame(self.root, style="Card.TFrame", padding=8)
        nav.grid(row=1, column=0, sticky="nsew", padx=(8, 4), pady=(4, 8))
        nav.columnconfigure(0, weight=1)
        nav.rowconfigure(8, weight=1)
        self.nav_frame = nav
        ttk.Label(nav, text=t("shell.nav.title"), style="Title.TLabel").grid(row=0, column=0, sticky="w", pady=(0, 12))
        ttk.Button(nav, text=t("shell.nav.run"), style="Nav.TButton", command=lambda: self.show_page("run")).grid(row=1, column=0, sticky="ew", pady=4)
        ttk.Button(nav, text=t("shell.nav.qc"), style="Nav.TButton", command=lambda: self.show_page("qc")).grid(row=2, column=0, sticky="ew", pady=4)
        ttk.Button(nav, text=t("shell.nav.results"), style="Nav.TButton", command=lambda: self.show_page("results")).grid(row=3, column=0, sticky="ew", pady=4)
        ttk.Button(nav, text=t("shell.nav.devices"), style="Nav.TButton", command=lambda: self.show_page("devices")).grid(row=4, column=0, sticky="ew", pady=4)
        ttk.Button(nav, text=t("shell.nav.algorithms"), style="Nav.TButton", command=lambda: self.show_page("algorithms")).grid(row=5, column=0, sticky="ew", pady=4)
        ttk.Button(nav, text=t("shell.nav.reports"), style="Nav.TButton", command=lambda: self.show_page("reports")).grid(row=6, column=0, sticky="ew", pady=4)
        ttk.Button(nav, text=t("shell.nav.plans"), style="Nav.TButton", command=lambda: self.show_page("plan")).grid(row=7, column=0, sticky="ew", pady=4)
        self.notification_center = NotificationCenter(nav)
        self.notification_center.grid(row=8, column=0, sticky="nsew", pady=(16, 0))

        self.main_split = ttk.Panedwindow(self.root, orient="vertical")
        self.main_split.grid(row=1, column=1, sticky="nsew", padx=(4, 8), pady=(4, 8))

        self.workspace = ttk.Frame(self.main_split, style="Card.TFrame")
        self.workspace.rowconfigure(0, weight=1)
        self.workspace.columnconfigure(0, weight=1)

        run_page = RunControlPage(
            self.workspace,
            controller=self.run_controller,
            initial_points_path=str(self.facade.config.paths.points_excel),
        )
        qc_page = QCPage(self.workspace)
        results_page = ResultsPage(self.workspace)
        devices_page = DevicesPage(self.workspace, facade=self.facade)
        algorithms_page = AlgorithmsPage(self.workspace)
        reports_page = ReportsPage(self.workspace, exporter=self.facade)
        plan_page = PlanEditorPage(self.workspace, facade=self.facade)
        self._pages = {
            "run": run_page,
            "qc": qc_page,
            "results": results_page,
            "devices": devices_page,
            "algorithms": algorithms_page,
            "reports": reports_page,
            "plan": plan_page,
        }
        for frame in self._pages.values():
            frame.grid(row=0, column=0, sticky="nsew")
        run_page.tkraise()
        self.current_page_name = "run"
        self.busy_overlay = BusyOverlay(self.workspace)
        self.busy_overlay.render({"active": False, "message": t("common.working")})

        self.log_panel = LogPanel(self.main_split)
        self.main_split.add(self.workspace, weight=6)
        self.main_split.add(self.log_panel, weight=1)
        self.root.after_idle(self._set_default_workspace_split)
        self._display_profile_sync_idle_after_id = self.root.after_idle(self._run_initial_display_profile_context_sync)
        self.main_split.bind("<ButtonRelease-1>", self._on_workspace_split_changed, add="+")
        self.root.bind("<Configure>", self._schedule_display_profile_context_sync, add="+")
        self.root.bind("<Destroy>", self._on_root_destroy, add="+")

    def _render_logs(self, logs: list[str]) -> None:
        if logs == self._last_logs:
            return
        self._last_logs = list(logs)
        self.log_panel.set_logs(logs[-200:])

    def _build_menu(self) -> tk.Menu:
        menu = tk.Menu(self.root, tearoff=False)
        file_menu = tk.Menu(menu, tearoff=False)
        file_menu.add_command(label=t("shell.menu.capture_screenshot"), command=self._capture_screenshot)
        recent_menu = tk.Menu(file_menu, tearoff=False)
        recent_runs = self.facade.get_recent_runs()
        if recent_runs:
            for item in recent_runs[:10]:
                path = str(item.get("path", "") or "")
                recent_menu.add_command(
                    label=path,
                    command=lambda value=path: self.facade.log_ui(t("shell.log.recent_run_selected", path=value)),
                )
        else:
            recent_menu.add_command(label=t("shell.menu.no_recent_runs"), state="disabled")
        file_menu.add_cascade(label=t("shell.menu.recent_runs"), menu=recent_menu)
        file_menu.add_separator()
        file_menu.add_command(label=t("shell.menu.exit"), command=self._close_shell)
        menu.add_cascade(label=t("shell.menu.file"), menu=file_menu)

        edit_menu = tk.Menu(menu, tearoff=False)
        edit_menu.add_command(label=t("shell.menu.preferences"), command=self.open_preferences_dialog)
        menu.add_cascade(label=t("shell.menu.edit"), menu=edit_menu)

        view_menu = tk.Menu(menu, tearoff=False)
        for label, page in (
            (t("shell.nav.run"), "run"),
            (t("shell.nav.qc"), "qc"),
            (t("shell.nav.results"), "results"),
            (t("shell.nav.devices"), "devices"),
            (t("shell.nav.algorithms"), "algorithms"),
            (t("shell.nav.reports"), "reports"),
            (t("shell.nav.plans"), "plan"),
        ):
            view_menu.add_command(label=label, command=lambda value=page: self.show_page(value))
        menu.add_cascade(label=t("shell.menu.view"), menu=view_menu)

        help_menu = tk.Menu(menu, tearoff=False)
        help_menu.add_command(label=t("shell.menu.about"), command=self.open_about_dialog)
        help_menu.add_command(label=t("shell.menu.release_notes"), command=self.open_release_notes_dialog)
        help_menu.add_command(label=t("shell.menu.licenses"), command=self.open_licenses_dialog)
        help_menu.add_separator()
        help_menu.add_command(label=t("shell.menu.export_diagnostics"), command=self.export_diagnostic_bundle)
        menu.add_cascade(label=t("shell.menu.help"), menu=help_menu)
        return menu

    def _capture_screenshot(self) -> None:
        preferences = self.facade.get_preferences()
        extension = str(preferences.get("screenshot_format", "png") or "png").strip().lower()
        if extension not in {"png", "jpg", "jpeg", "txt"}:
            extension = "png"
        target = self.facade.runtime_paths.screenshots_dir / f"{self.facade.session.run_id}_shell.{extension}"
        path = export_widget_screenshot(self.root, target)
        self.facade.log_ui(t("shell.log.screenshot_exported", path=path))

    def _close_shell(self) -> None:
        self.shutdown()
        self.root.destroy()

    def _apply_initial_geometry(self) -> None:
        try:
            screen_width = int(self.root.winfo_screenwidth() or 0)
            screen_height = int(self.root.winfo_screenheight() or 0)
        except tk.TclError:
            return
        if screen_width <= 0 or screen_height <= 0:
            return
        width = min(max(1440, int(screen_width * 0.86)), max(1440, screen_width - 120))
        height = min(max(860, int(screen_height * 0.88)), max(860, screen_height - 100))
        offset_x = max(20, int((screen_width - width) / 2))
        offset_y = max(20, int((screen_height - height) / 2))
        self.root.geometry(f"{width}x{height}+{offset_x}+{offset_y}")

    def _set_default_workspace_split(self) -> None:
        try:
            total_height = int(self.main_split.winfo_height() or 0)
            if total_height <= 0:
                return
            remembered = self._remembered_workspace_split(total_height)
            if remembered is None:
                log_height = max(118, min(168, int(total_height * 0.18)))
                remembered = max(520, total_height - log_height)
            self._restoring_workspace_split = True
            self.main_split.sashpos(0, remembered)
        except Exception:
            return
        finally:
            self._restoring_workspace_split = False

    def _remembered_workspace_split(self, total_height: int) -> int | None:
        try:
            preferences = self.facade.get_preferences()
            raw_value = preferences.get("shell_log_sash")
            if raw_value in ("", None):
                return None
            parsed = int(raw_value)
        except Exception:
            return None
        minimum = 420
        maximum = max(minimum + 120, total_height - 90)
        return max(minimum, min(maximum, parsed))

    def _on_workspace_split_changed(self, _event: tk.Event[tk.Misc]) -> None:
        if self._restoring_workspace_split:
            return
        self.root.after_idle(self._remember_workspace_split)

    def _remember_workspace_split(self) -> None:
        try:
            position = int(self.main_split.sashpos(0))
        except Exception:
            return
        try:
            self.facade.save_ui_layout_preferences({"shell_log_sash": position})
        except Exception:
            return

    def _sync_display_profile_context(self) -> None:
        if not self._display_profile_sync_root_available():
            return
        try:
            screen_width = int(self.root.winfo_screenwidth() or 0)
            screen_height = int(self.root.winfo_screenheight() or 0)
            window_width = int(self.root.winfo_width() or 0)
            window_height = int(self.root.winfo_height() or 0)
        except Exception:
            return
        if min(screen_width, screen_height, window_width, window_height) <= 0:
            return
        metrics = (screen_width, screen_height, window_width, window_height)
        if metrics == self._last_display_profile_metrics:
            return
        self._last_display_profile_metrics = metrics
        try:
            result = self.facade.execute_device_workbench_action(
                "workbench",
                "refresh_display_profile_context",
                screen_width=screen_width,
                screen_height=screen_height,
                window_width=window_width,
                window_height=window_height,
            )
        except Exception:
            return
        snapshot = dict(result.get("snapshot") or {})
        if snapshot:
            try:
                self._pages["devices"].render(snapshot)
            except Exception:
                return

    def _display_profile_sync_root_available(self) -> bool:
        if self._shell_shutdown or self._root_destroyed:
            return False
        if getattr(self, "root", None) is None or getattr(self.root, "tk", None) is None:
            return False
        try:
            if not bool(int(self.root.winfo_exists())):
                return False
            self.root.tk.call("after", "info")
        except Exception:
            return False
        return True

    def _cancel_display_profile_context_sync_callbacks(self) -> None:
        for attr_name in ("_display_profile_sync_idle_after_id", "_display_profile_sync_after_id"):
            after_id = getattr(self, attr_name, None)
            if not after_id:
                continue
            try:
                self.root.after_cancel(after_id)
            except Exception:
                pass
            setattr(self, attr_name, None)

    def _run_initial_display_profile_context_sync(self) -> None:
        self._display_profile_sync_idle_after_id = None
        self._run_scheduled_display_profile_context_sync()

    def _schedule_display_profile_context_sync(self, _event: tk.Event[tk.Misc] | None = None) -> None:
        if not self._display_profile_sync_root_available():
            self._display_profile_sync_after_id = None
            return
        if self._display_profile_sync_after_id:
            try:
                self.root.after_cancel(self._display_profile_sync_after_id)
            except Exception:
                pass
            self._display_profile_sync_after_id = None
        try:
            self._display_profile_sync_after_id = self.root.after(120, self._run_scheduled_display_profile_context_sync)
        except Exception:
            self._display_profile_sync_after_id = None

    def _run_scheduled_display_profile_context_sync(self) -> None:
        self._display_profile_sync_after_id = None
        if not self._display_profile_sync_root_available():
            return
        self._sync_display_profile_context()

    def _on_root_destroy(self, event: tk.Event[tk.Misc]) -> None:
        if event.widget is not self.root:
            return
        self._root_destroyed = True
        self._cancel_display_profile_context_sync_callbacks()

    def _prompt_crash_recovery_if_needed(self) -> None:
        if self.crash_recovery is None or not self._recovery_prompt_pending:
            self._recovery_prompt_pending = False
            return
        snapshot = self.crash_recovery.load_pending_snapshot() or {}
        should_restore = bool(
            messagebox.askyesno(
                t("shell.recovery.title"),
                self.crash_recovery.build_prompt(snapshot),
                parent=self.root,
            )
        )
        if should_restore:
            page_name = str(snapshot.get("current_page", "run") or "run")
            if page_name in self._pages:
                self.show_page(page_name)
            self.facade.log_ui(t("shell.log.previous_session_restored"))
        else:
            self.facade.log_ui(t("shell.log.previous_session_recovery_skipped"))
        self._recovery_prompt_pending = False
        self._refresh_recovery_snapshot()

    def _refresh_recovery_snapshot(self, snapshot: dict[str, Any] | None = None) -> None:
        if self.crash_recovery is None or self._recovery_prompt_pending:
            return
        payload = snapshot or self.facade.build_snapshot()
        run = dict(payload.get("run", {}) or {})
        signature = (
            self.current_page_name,
            str(run.get("run_id", "")),
            str(run.get("phase", "")),
            str(run.get("message", "")),
            str(run.get("current_point", "")),
            str(run.get("route", "")),
            str(run.get("progress_pct", "")),
        )
        if signature == self._last_recovery_signature:
            return
        self._last_recovery_signature = signature
        self.crash_recovery.save_ui_snapshot(
            current_page=self.current_page_name,
            ui_snapshot=payload,
            logs=self._last_logs,
        )
