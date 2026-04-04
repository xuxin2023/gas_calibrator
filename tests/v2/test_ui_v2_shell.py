from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.app import build_application
from gas_calibrator.v2.ui_v2.i18n import t
from gas_calibrator.v2.ui_v2.runtime.recovery_store import RecoveryStore

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import build_fake_facade, make_root


def _collect_widget_texts(widget) -> list[str]:
    texts: list[str] = []
    for child in widget.winfo_children():
        text = ""
        try:
            text = str(child.cget("text") or "")
        except Exception:
            text = ""
        if text:
            texts.append(text)
        texts.extend(_collect_widget_texts(child))
    return texts


def test_shell_registers_new_pages_and_navigation(tmp_path: Path) -> None:
    root = make_root()
    shell = None
    try:
        facade = build_fake_facade(tmp_path)
        facade.runtime_paths.route_memory_path.write_text('{"last_page": "reports"}', encoding="utf-8")
        _, shell, _ = build_application(root=root, facade=facade, start_feed=False)
        assert set(shell._pages) == {"run", "qc", "results", "devices", "algorithms", "reports", "plan"}
        assert shell.main_split is not None
        assert shell.error_banner.visible is False
        assert shell.notification_center is not None
        assert shell.busy_overlay.visible is False
        assert shell.current_page_name == "reports"
        assert root["menu"]
        assert root.title() == t("shell.title")
        all_texts = _collect_widget_texts(root)
        for label in (
            t("shell.nav.run"),
            t("shell.nav.qc"),
            t("shell.nav.results"),
            t("shell.nav.devices"),
            t("shell.nav.algorithms"),
            t("shell.nav.reports"),
            t("shell.nav.plans"),
            t("widgets.notification.title"),
        ):
            assert label in all_texts
        assert shell.notification_center.empty_state.title_var.get() == t("widgets.notification.empty_title")

        shell.show_page("devices")
        root.update_idletasks()
        assert shell.current_page_name == "devices"

        shell.show_page("reports")
        root.update_idletasks()
        assert shell.current_page_name == "reports"
        shell.show_page("plan")
        root.update_idletasks()
        assert shell.current_page_name == "plan"
        shell.open_preferences_dialog()
        shell.open_about_dialog()
        shell.open_release_notes_dialog()
        shell.open_licenses_dialog()
        export_result = shell.export_diagnostic_bundle()
        assert shell.preferences_dialog is not None
        assert shell.about_dialog is not None
        assert shell.release_notes_dialog is not None
        assert shell.licenses_dialog is not None
        assert export_result["ok"] is True
    finally:
        if shell is not None:
            shell.shutdown()
        root.destroy()


def test_shell_restores_previous_ui_context_when_recovery_is_accepted(tmp_path: Path, monkeypatch) -> None:
    root = make_root()
    try:
        facade = build_fake_facade(tmp_path)
        store = RecoveryStore.from_runtime_paths(facade.runtime_paths)
        store.save(
            {
                "saved_at": "2026-03-22T00:00:00+00:00",
                "run_id": facade.session.run_id,
                "phase": "co2_route",
                "message": "previous crash",
                "current_page": "devices",
            }
        )
        monkeypatch.setattr("gas_calibrator.v2.ui_v2.shell.messagebox.askyesno", lambda *args, **kwargs: True)

        _, shell, _ = build_application(root=root, facade=facade, start_feed=False)

        assert shell.current_page_name == "devices"
        shell.shutdown()
        assert store.exists() is False
    finally:
        root.destroy()


def test_shell_syncs_display_profile_context_from_window_metrics(tmp_path: Path, monkeypatch) -> None:
    root = make_root()
    shell = None
    try:
        facade = build_fake_facade(tmp_path)
        expected_root = tmp_path / "expected_contract"
        expected_root.mkdir()
        expected = build_fake_facade(expected_root).execute_device_workbench_action(
            "workbench",
            "refresh_display_profile_context",
            screen_width=2560,
            screen_height=1440,
            window_width=2200,
            window_height=1200,
        )["snapshot"]["meta"]["display_profile_meta"]
        monkeypatch.setattr(root, "winfo_screenwidth", lambda: 2560)
        monkeypatch.setattr(root, "winfo_screenheight", lambda: 1440)
        monkeypatch.setattr(root, "winfo_width", lambda: 2200)
        monkeypatch.setattr(root, "winfo_height", lambda: 1200)

        _, shell, _ = build_application(root=root, facade=facade, start_feed=False)
        root.update_idletasks()
        root.update()

        context = facade.get_preferences()["workbench"]["display_profile_context"]
        assert context["screen_width"] == 2560
        assert context["screen_height"] == 1440
        assert context["window_width"] == 2200
        assert context["window_height"] == 1200
        assert context["resolved"] == expected["resolved"] == "1440p_standard"
        assert context["family"] == expected["profile_family"] == "1440p"
        assert context["resolution_bucket"] == expected["resolution_bucket"] == "1440p"
        assert context["monitor_class"] == expected["monitor_class"] == "wide_monitor"
        assert context["window_class"] == expected["window_class"] == "wide_window"
        assert context["strategy_version"] == "display_profile_v2"
        assert context["resolution_class"] == expected["resolution_class"] == "wide_resolution"
        assert context["auto_reason"] == expected["auto_reason"] == "simulated_1440p_canvas"
        assert context["multi_monitor_ready_hint"] == expected["multi_monitor_ready_hint"] == "future_multi_monitor_ready"
    finally:
        if shell is not None:
            shell.shutdown()
        root.destroy()


def test_shell_debounces_configure_driven_display_profile_refresh(tmp_path: Path, monkeypatch) -> None:
    root = make_root()
    shell = None
    try:
        facade = build_fake_facade(tmp_path)
        _, shell, _ = build_application(root=root, facade=facade, start_feed=False)
        shell._cancel_display_profile_context_sync_callbacks()

        queued: list[tuple[str, object, tuple[object, ...]]] = []
        cancelled: list[str] = []

        def _fake_after(delay: int, callback=None, *args):
            token = f"after-{len(queued) + 1}"
            queued.append((token, callback, args))
            return token

        def _fake_after_cancel(token: str) -> None:
            cancelled.append(token)

        monkeypatch.setattr(root, "after", _fake_after)
        monkeypatch.setattr(root, "after_cancel", _fake_after_cancel)
        monkeypatch.setattr(root, "winfo_screenwidth", lambda: 1920)
        monkeypatch.setattr(root, "winfo_screenheight", lambda: 1080)
        monkeypatch.setattr(root, "winfo_width", lambda: 1760)
        monkeypatch.setattr(root, "winfo_height", lambda: 980)

        shell._schedule_display_profile_context_sync()
        shell._schedule_display_profile_context_sync()

        assert "after-1" in cancelled
        assert queued[-1][0] == "after-2"

        _, callback, args = queued[-1]
        assert callback is not None
        callback(*args)

        context = facade.get_preferences()["workbench"]["display_profile_context"]
        assert context["screen_width"] == 1920
        assert context["screen_height"] == 1080
        assert context["window_width"] == 1760
        assert context["window_height"] == 980
        assert context["resolved"] == "1080p_standard"
        assert context["auto_reason"] == "default_1080p"
    finally:
        if shell is not None:
            shell.shutdown()
        root.destroy()


def test_shell_shutdown_cancels_pending_display_profile_sync_and_blocks_late_execution(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = make_root()
    shell = None
    try:
        facade = build_fake_facade(tmp_path)
        _, shell, _ = build_application(root=root, facade=facade, start_feed=False)
        shell._cancel_display_profile_context_sync_callbacks()

        queued: list[str] = []
        cancelled: list[str] = []
        sync_calls: list[str] = []

        def _fake_after(delay: int, callback=None, *args):
            token = f"after-{len(queued) + 1}"
            queued.append(token)
            return token

        def _fake_after_cancel(token: str) -> None:
            cancelled.append(token)

        monkeypatch.setattr(root, "after", _fake_after)
        monkeypatch.setattr(root, "after_cancel", _fake_after_cancel)
        monkeypatch.setattr(shell, "_sync_display_profile_context", lambda: sync_calls.append("sync"))

        shell._display_profile_sync_idle_after_id = "idle-1"
        shell._schedule_display_profile_context_sync()

        assert shell._display_profile_sync_after_id == "after-1"

        shell.shutdown()

        assert "idle-1" in cancelled
        assert "after-1" in cancelled
        assert shell._display_profile_sync_after_id is None
        assert shell._display_profile_sync_idle_after_id is None

        shell._run_scheduled_display_profile_context_sync()
        shell._schedule_display_profile_context_sync()

        assert sync_calls == []
        assert queued == ["after-1"]
    finally:
        root.destroy()


def test_shell_skips_display_profile_schedule_when_root_is_unavailable(tmp_path: Path, monkeypatch) -> None:
    root = make_root()
    shell = None
    try:
        facade = build_fake_facade(tmp_path)
        _, shell, _ = build_application(root=root, facade=facade, start_feed=False)
        shell._cancel_display_profile_context_sync_callbacks()

        queued: list[str] = []
        sync_calls: list[str] = []

        monkeypatch.setattr(root, "after", lambda delay, callback=None, *args: queued.append(f"after-{delay}") or "after-1")
        monkeypatch.setattr(root, "winfo_exists", lambda: 0)
        monkeypatch.setattr(shell, "_sync_display_profile_context", lambda: sync_calls.append("sync"))

        shell._schedule_display_profile_context_sync()
        shell._run_scheduled_display_profile_context_sync()

        assert queued == []
        assert sync_calls == []
    finally:
        if shell is not None:
            shell.shutdown()
        root.destroy()


def test_shell_destroy_has_no_display_profile_teardown_noise(tmp_path: Path, capsys) -> None:
    root = make_root()
    shell = None
    try:
        facade = build_fake_facade(tmp_path)
        _, shell, _ = build_application(root=root, facade=facade, start_feed=False)
        shell._schedule_display_profile_context_sync()
    finally:
        if shell is not None:
            shell.shutdown()
        root.destroy()

    captured = capsys.readouterr()
    assert "invalid command name" not in captured.err
    assert "_run_scheduled_display_profile_context_sync" not in captured.err
