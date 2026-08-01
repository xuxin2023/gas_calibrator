from __future__ import annotations

import tkinter as tk
import time
import json
from pathlib import Path

import pytest

from gas_calibrator.v1_5.ui import operator_workstation_app as workstation_ui
from gas_calibrator.v1_5.ui.operator_workstation_app import (
    OperatorWorkstationApp,
    _t,
    main as workstation_main,
)
from gas_calibrator.v1_5.ui.pages.visitor_showcase_page import VisitorShowcasePage
from gas_calibrator.v1_5.ui.screenshot import export_widget_screenshot
from gas_calibrator.v1_5.ui.scrollable_page_frame import ScrollablePageFrame
from v1_5_workstation_test_support import (
    write_decision_authority_archive,
    write_legacy_profile_queues,
)


def _root() -> tk.Tk:
    try:
        root = tk.Tk()
    except tk.TclError as exc:  # pragma: no cover - environment guard
        pytest.skip(f"Tk unavailable: {exc}")
    root.withdraw()
    return root


def _texts(widget: tk.Misc) -> list[str]:
    values: list[str] = []
    for child in widget.winfo_children():
        try:
            text = str(child.cget("text") or "")
        except (tk.TclError, AttributeError):
            text = ""
        if text:
            values.append(text)
        values.extend(_texts(child))
    return values


def test_v1_5_visitor_showcase_is_read_only_and_1080p_ready() -> None:
    root = _root()
    events: list[str] = []
    try:
        page = VisitorShowcasePage(
            root,
            on_enter_presentation=lambda: events.append("enter"),
            on_exit_presentation=lambda: events.append("exit"),
        )
        page.pack(fill="both", expand=True)
        root.geometry("1920x1080+0+0")
        root.deiconify()
        root.update_idletasks()
        root.update()
        page.render({"run": {"phase_display": "仿真"}})
        root.update_idletasks()

        texts = [
            str(page.canvas.itemcget(item, "text") or "")
            for item in page.canvas.find_all()
            if page.canvas.type(item) == "text"
        ]
        joined = "\n".join(texts)
        assert "V1.5 气体分析仪智能校准中心" in joined
        assert "非真机验收证据" in joined
        assert "仿真示意曲线 · 非实时设备数据" in joined
        assert "开始校准" not in joined
        assert "停止校准" not in joined
        assert page.canvas.winfo_width() >= 1180
        assert page.canvas.winfo_height() >= 720

        page._toggle_presentation()
        assert events == ["enter"]
        page.set_presentation_active(True)
        page._toggle_presentation()
        assert events == ["enter", "exit"]
    finally:
        root.destroy()


def test_v1_5_screenshot_helper_exports_fallback_artifact(tmp_path) -> None:
    root = _root()
    try:
        root.update_idletasks()
        path = export_widget_screenshot(root, tmp_path / "capture.txt")
        assert path.exists()
    finally:
        root.destroy()


def test_v1_5_scroll_scaffold_exposes_overflow_scrollbar() -> None:
    root = _root()
    try:
        scaffold = ScrollablePageFrame(root, style="TFrame")
        scaffold.pack(fill="both", expand=True)
        for index in range(80):
            tk.Label(scaffold.content, text=f"row {index}").grid(
                row=index,
                column=0,
                sticky="w",
            )
        root.geometry("800x400+0+0")
        root.deiconify()
        root.update_idletasks()
        root.update()

        assert scaffold.has_overflow()
        assert scaffold.is_scrollbar_visible()
        scaffold.scroll_to_top()
        assert scaffold.canvas.yview()[0] == pytest.approx(0.0)
    finally:
        root.destroy()


def test_operator_workstation_is_v1_5_first_chinese_and_1080p_ready() -> None:
    root = _root()
    try:
        app = OperatorWorkstationApp(root)
        root.geometry("1920x1080+0+0")
        root.deiconify()
        root.update_idletasks()
        root.update()
        joined = "\n".join(_texts(root))

        assert root.title() == "V1.5 气体分析仪校准工作站"
        assert "生产校准内核：0613 / 0620 / 0621" in joined
        assert "成熟流程 45 CO₂ 点 + 13 H₂O 点" in joined
        assert "CO₂ 零气锚点" in joined
        assert "H₂O 干气锚点" in joined
        assert "证书资料不阻断启动" in joined
        assert "分析、报告与证据" in joined
        assert "24.9 °C / 等待" not in joined
        assert "1000 hPa / -45 °C" not in joined
        assert "未知｜未发现可信新鲜工件" in joined
        assert "V2" not in joined
        assert "V2 驾驶舱" not in joined
        assert "开始演练" in joined
        assert "查看受控交接预览" in joined
        assert "开始真实校准" not in joined
        assert root.winfo_width() >= 1800
        assert root.winfo_height() >= 1000
        assert app.start_button.instate(["!disabled"])
        aside_bottom = max(
            child.winfo_y() + child.winfo_height()
            for child in app.aside_frame.winfo_children()
        )
        assert aside_bottom <= app.aside_frame.winfo_height()
        app.open_settings()
        root.update_idletasks()
        assert app._settings_dialog is not None
        settings_text = _texts(app._settings_dialog)
        assert _t("dialog.authority_operator") in settings_text
        assert settings_text.count(_t("dialog.browse")) == 7
        assert app._settings_dialog.winfo_height() <= 1080
    finally:
        root.destroy()


def test_controlled_handoff_preview_is_read_only_and_keeps_double_unlock(
    tmp_path,
    monkeypatch,
) -> None:
    root = _root()
    repository_root = Path(__file__).resolve().parents[1]
    co2, h2o = write_legacy_profile_queues(tmp_path)
    preflight_calls: list[dict] = []
    info_messages: list[str] = []
    error_messages: list[str] = []
    receipt_path = tmp_path / "h2o_preflight_receipt.json"
    real_preflight = workstation_ui.preflight_v1_5_controlled_mature_route

    def recording_preflight(plan, **kwargs):
        preflight_calls.append(dict(kwargs))
        return real_preflight(plan, **kwargs)

    monkeypatch.setattr(
        workstation_ui,
        "preflight_v1_5_controlled_mature_route",
        recording_preflight,
    )
    monkeypatch.setattr(
        workstation_ui.filedialog,
        "asksaveasfilename",
        lambda **_kwargs: str(receipt_path),
    )
    monkeypatch.setattr(
        workstation_ui.messagebox,
        "showinfo",
        lambda _title, message, **_kwargs: info_messages.append(str(message)),
    )
    monkeypatch.setattr(
        workstation_ui.messagebox,
        "showerror",
        lambda _title, message, **_kwargs: error_messages.append(str(message)),
    )
    try:
        app = OperatorWorkstationApp(
            root,
            initial_settings={
                "config": str(repository_root / "configs" / "default_config.json"),
                "co2": str(co2),
                "h2o": str(h2o),
                "output": str(tmp_path / "output"),
            },
        )
        app.open_controlled_handoff_preview()
        root.update_idletasks()

        assert app._handoff_dialog is not None
        assert app._handoff_dialog.winfo_exists()
        assert app._handoff_preview_widget is not None
        assert str(app._handoff_preview_widget.cget("state")) == "disabled"
        preview = app._handoff_preview_widget.get("1.0", "end")
        assert _t("handoff.authority.not_configured") in preview
        assert "状态：等待显式双重解锁" in preview
        assert "执行权限：否" in preview
        assert "正式验收证据：否" in preview
        assert "可选范围：仅响应检查｜仿真已就绪，真实执行锁定" in preview
        assert "不切气路、不设压力、不改变 MODE、不采校准点、不写设备" in preview
        assert "仿真演练启动：允许" in preview
        assert "真实执行启动：锁定" in preview
        assert "受控系数写入：锁定" in preview
        assert "正式证书签发：锁定" in preview
        assert "--engineering-probe-only" in preview
        assert "--no-ftd-write" in preview
        assert "<OPERATOR_CONFIRMATION_REQUIRED_AT_EXECUTION>" in preview
        dialog_text = "\n".join(_texts(app._handoff_dialog))
        assert app.controlled_route_var.get() == "CO₂"
        assert app.controlled_preflight_button is not None
        assert app.controlled_preflight_receipt_button is not None
        assert app.controlled_preflight_receipt_button.instate(["disabled"])
        assert str(app.controlled_preflight_button.cget("text")) == _t(
            "handoff.preflight.run"
        )
        assert _t("handoff.preflight.locked") in dialog_text

        co2_preflight = app.run_controlled_route_preflight()
        assert co2_preflight["route_kind"] == "co2"
        assert co2_preflight["status"] == "preflight_ready_execution_locked"
        assert co2_preflight["execution_started"] is False
        assert co2_preflight["execution_allowed"] is False
        assert co2_preflight["runner_invocation_count"] == 0
        assert "预检通过" in app.controlled_preflight_var.get()
        assert app.controlled_preflight_receipt_button.instate(["!disabled"])

        app.controlled_route_var.set("H₂O")
        h2o_preflight = app.run_controlled_route_preflight()
        assert h2o_preflight["route_kind"] == "h2o"
        assert h2o_preflight["status"] == "preflight_ready_execution_locked"
        assert h2o_preflight["runner_invocation_count"] == 0
        assert [call["route_kind"] for call in preflight_calls] == ["co2", "h2o"]
        assert app._handoff_dialog.winfo_height() <= 1080

        written = app.save_controlled_route_preflight_receipt()
        assert written is not None
        assert written["preflight_passed"] is True
        payload = json.loads(receipt_path.read_text(encoding="utf-8"))
        assert payload["route_binding"]["route_kind"] == "h2o"
        assert payload["route_binding"]["queue_csv"]["point_count"] == 13
        assert payload["real_execution_authorized"] is False
        original = receipt_path.read_bytes()
        assert info_messages and "SHA-256" in info_messages[-1]

        assert app.save_controlled_route_preflight_receipt() is None
        assert receipt_path.read_bytes() == original
        assert error_messages and "FileExistsError" in error_messages[-1]

        dialog_text = "\n".join(_texts(app._handoff_dialog))
        assert "此窗口不执行任何命令" in dialog_text
        assert "关闭" in dialog_text
        assert any(
            widget.winfo_class() == "TScrollbar"
            for frame in app._handoff_dialog.winfo_children()
            for widget in frame.winfo_children()
        )
        assert "执行" not in {
            str(child.cget("text"))
            for child in app._handoff_dialog.winfo_children()
            if child.winfo_class() == "TButton"
        }
    finally:
        root.destroy()


def test_operator_workstation_settings_are_editable_and_certificate_optional() -> None:
    root = _root()
    try:
        app = OperatorWorkstationApp(root)
        app.open_settings()
        root.update_idletasks()
        assert app._settings_dialog is not None
        assert app._settings_dialog.winfo_exists()
        assert app.settings["config"].get().endswith("configs\\default_config.json")
        assert app.settings["co2"].get().endswith("co2_runner_queue.csv")
        assert app.settings["h2o"].get().endswith("h2o_runner_queue.csv")
        assert app.settings["runtime"].get().endswith("\\logs")
        assert app.settings["certificate"].get() == ""
        assert app.settings["authority_archive"].get() == ""
        assert app.settings["authority_run_id"].get() == ""
        assert app.settings["authority_device_ids"].get() == ""
        assert _t("dialog.authority_archive") in "\n".join(
            _texts(app._settings_dialog)
        )
        assert _t("dialog.authority_run_id") in "\n".join(
            _texts(app._settings_dialog)
        )
        assert _t("dialog.authority_device_ids") in "\n".join(
            _texts(app._settings_dialog)
        )
        assert "COM23" in app.config_gate_var.get()
        assert "COM22" in app.config_gate_var.get()
        assert app.config_hash_var.get().startswith("SHA256 ")
        app._settings_dialog.destroy()
    finally:
        root.destroy()


def test_fixed_startup_preflight_accepts_config_and_45_13_queues(
    tmp_path,
    capsys,
) -> None:
    root = Path(__file__).resolve().parents[1]
    co2, h2o = write_legacy_profile_queues(tmp_path)

    rc = workstation_main(
        [
            "--config",
            str(root / "configs" / "default_config.json"),
            "--co2-queue-csv",
            str(co2),
            "--h2o-queue-csv",
            str(h2o),
            "--output-dir",
            str(tmp_path / "output"),
            "--validate-startup-only",
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "ready_for_v1_5_dry_run"
    assert payload["point_counts"] == {"co2": 45, "h2o": 13}
    assert payload["opens_com_ports"] is False
    assert (
        payload["controlled_execution_handoff"]["status"]
        == "blocked_pending_explicit_double_unlock"
    )
    assert payload["controlled_execution_handoff"]["default_scope"] == "response_only"
    assert payload["decision_model"]["aggregate_status"] == (
        "simulation_ready_real_locked"
    )
    assert payload["decision_model"]["can_start_simulation"] is True
    assert payload["decision_model"]["can_start_real_execution"] is False
    assert payload["decision_authority_binding"]["status"] == "not_configured"


def test_fixed_startup_preflight_binds_authorities_by_archive_hash(
    tmp_path,
    capsys,
) -> None:
    root = Path(__file__).resolve().parents[1]
    co2, h2o = write_legacy_profile_queues(tmp_path)
    archive, _, _ = write_decision_authority_archive(tmp_path)

    rc = workstation_main(
        [
            "--config",
            str(root / "configs" / "default_config.json"),
            "--co2-queue-csv",
            str(co2),
            "--h2o-queue-csv",
            str(h2o),
            "--decision-authority-archive-json",
            str(archive),
            "--expected-authority-run-id",
            "formal_batch_001",
            "--expected-authority-device-ids",
            "001,002",
            "--validate-startup-only",
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["decision_authority_binding"]["status"] == "ready"
    assert payload["decision_authority_binding"]["identity_binding"][
        "status"
    ] == "ready"
    assert payload["decision_model"]["can_start_real_execution"] is False
    assert payload["decision_model"]["can_write_coefficients"] is True
    assert payload["decision_model"]["can_issue_formal_certificate"] is True
    assert payload["opens_com_ports"] is False


def test_fixed_startup_preflight_writes_locked_receipt(
    tmp_path,
    capsys,
) -> None:
    root = Path(__file__).resolve().parents[1]
    co2, h2o = write_legacy_profile_queues(tmp_path)
    receipt_path = tmp_path / "startup_receipt.json"

    rc = workstation_main(
        [
            "--config",
            str(root / "configs" / "default_config.json"),
            "--co2-queue-csv",
            str(co2),
            "--h2o-queue-csv",
            str(h2o),
            "--output-dir",
            str(tmp_path / "output"),
            "--startup-receipt-json",
            str(receipt_path),
            "--validate-startup-only",
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["startup_receipt"]["path"] == str(receipt_path.resolve())
    assert payload["startup_receipt"]["sha256"]
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["probe_execution_allowed"] is False
    assert receipt["operator_acknowledgement_template"]["completed"] is False
    assert receipt["opens_com_ports"] is False


@pytest.mark.parametrize(
    ("route_kind", "expected_point_count"),
    [("co2", 45), ("h2o", 13)],
)
def test_fixed_startup_preflight_writes_controlled_route_receipt_without_tk(
    tmp_path,
    capsys,
    monkeypatch,
    route_kind,
    expected_point_count,
) -> None:
    root = Path(__file__).resolve().parents[1]
    co2, h2o = write_legacy_profile_queues(tmp_path)
    receipt_path = tmp_path / f"{route_kind}_preflight_receipt.json"
    monkeypatch.setattr(
        workstation_ui,
        "build_application",
        lambda **_kwargs: pytest.fail("controlled preflight must not construct Tk"),
    )

    rc = workstation_main(
        [
            "--config",
            str(root / "configs" / "default_config.json"),
            "--co2-queue-csv",
            str(co2),
            "--h2o-queue-csv",
            str(h2o),
            "--output-dir",
            str(tmp_path / "output"),
            "--controlled-route-preflight-route",
            route_kind,
            "--controlled-route-preflight-receipt-json",
            str(receipt_path),
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    preflight = payload["controlled_route_preflight"]
    assert preflight["route_kind"] == route_kind
    assert preflight["status"] == "preflight_ready_execution_locked"
    assert preflight["execution_started"] is False
    assert preflight["execution_allowed"] is False
    assert preflight["runner_invocation_count"] == 0
    written = payload["controlled_route_preflight_receipt"]
    assert written["path"] == str(receipt_path.resolve())
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["route_binding"]["route_kind"] == route_kind
    assert receipt["route_binding"]["queue_csv"]["point_count"] == (
        expected_point_count
    )
    assert receipt["real_execution_authorized"] is False
    assert receipt["opens_com_ports"] is False


def test_fixed_startup_preflight_requires_route_for_controlled_receipt(
    tmp_path,
    capsys,
    monkeypatch,
) -> None:
    receipt_path = tmp_path / "missing_route_receipt.json"
    monkeypatch.setattr(
        workstation_ui,
        "build_application",
        lambda **_kwargs: pytest.fail("invalid preflight must not construct Tk"),
    )

    rc = workstation_main(
        [
            "--controlled-route-preflight-receipt-json",
            str(receipt_path),
        ]
    )

    assert rc == 2
    payload = json.loads(capsys.readouterr().err)
    assert "controlled_route_preflight_receipt_requires_route" in payload[
        "blockers"
    ]
    assert payload["opens_com_ports"] is False
    assert not receipt_path.exists()


def test_fixed_startup_preflight_refuses_controlled_receipt_overwrite(
    tmp_path,
    capsys,
    monkeypatch,
) -> None:
    root = Path(__file__).resolve().parents[1]
    co2, h2o = write_legacy_profile_queues(tmp_path)
    receipt_path = tmp_path / "co2_preflight_receipt.json"
    receipt_path.write_text("preserve-me", encoding="utf-8")
    monkeypatch.setattr(
        workstation_ui,
        "build_application",
        lambda **_kwargs: pytest.fail("controlled preflight must not construct Tk"),
    )

    rc = workstation_main(
        [
            "--config",
            str(root / "configs" / "default_config.json"),
            "--co2-queue-csv",
            str(co2),
            "--h2o-queue-csv",
            str(h2o),
            "--controlled-route-preflight-route",
            "co2",
            "--controlled-route-preflight-receipt-json",
            str(receipt_path),
        ]
    )

    assert rc == 2
    payload = json.loads(capsys.readouterr().err)
    assert payload["blockers"] == [
        "controlled_route_preflight_receipt_write_failed:FileExistsError"
    ]
    assert payload["opens_com_ports"] is False
    assert receipt_path.read_text(encoding="utf-8") == "preserve-me"


def test_fixed_startup_preflight_refuses_receipt_overwrite(
    tmp_path,
    capsys,
) -> None:
    receipt_path = tmp_path / "startup_receipt.json"
    receipt_path.write_text("preserve-me", encoding="utf-8")

    rc = workstation_main(
        [
            "--startup-receipt-json",
            str(receipt_path),
            "--validate-startup-only",
        ]
    )

    assert rc == 2
    payload = json.loads(capsys.readouterr().err)
    assert payload["blockers"] == [
        "startup_receipt_write_failed:FileExistsError"
    ]
    assert payload["opens_com_ports"] is False
    assert receipt_path.read_text(encoding="utf-8") == "preserve-me"


def test_fixed_startup_preflight_writes_confirmed_archive_receipt(
    tmp_path,
    capsys,
) -> None:
    root = Path(__file__).resolve().parents[1]
    co2, h2o = write_legacy_profile_queues(tmp_path)
    archive, _, _ = write_decision_authority_archive(tmp_path)
    receipt_path = tmp_path / "archive_confirmation.json"

    rc = workstation_main(
        [
            "--config",
            str(root / "configs" / "default_config.json"),
            "--co2-queue-csv",
            str(co2),
            "--h2o-queue-csv",
            str(h2o),
            "--decision-authority-archive-json",
            str(archive),
            "--expected-authority-run-id",
            "formal_batch_001",
            "--expected-authority-device-ids",
            "001,002",
            "--authority-confirmation-receipt-json",
            str(receipt_path),
            "--authority-confirmation-operator",
            "operator-cli",
            "--authority-confirmation-text",
            workstation_ui.V1_5_ARCHIVE_AUTHORITY_CONFIRMATION_TEXT,
            "--validate-startup-only",
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    summary = payload["authority_confirmation_receipt"]
    assert summary["status"] == "confirmed"
    assert summary["confirmation_valid"] is True
    assert summary["opens_com_ports"] is False
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["operator_confirmation"]["operator_name"] == "operator-cli"
    assert receipt["formal_actions_unlocked_by_receipt"] is False
    assert receipt["not_real_acceptance_evidence"] is True


def test_fixed_startup_preflight_writes_blocked_archive_receipt_on_bad_text(
    tmp_path,
    capsys,
) -> None:
    root = Path(__file__).resolve().parents[1]
    co2, h2o = write_legacy_profile_queues(tmp_path)
    archive, _, _ = write_decision_authority_archive(tmp_path)
    receipt_path = tmp_path / "blocked_archive_confirmation.json"

    rc = workstation_main(
        [
            "--config",
            str(root / "configs" / "default_config.json"),
            "--co2-queue-csv",
            str(co2),
            "--h2o-queue-csv",
            str(h2o),
            "--decision-authority-archive-json",
            str(archive),
            "--expected-authority-run-id",
            "formal_batch_001",
            "--expected-authority-device-ids",
            "001,002",
            "--authority-confirmation-receipt-json",
            str(receipt_path),
            "--authority-confirmation-operator",
            "operator-cli",
            "--authority-confirmation-text",
            "CONFIRM",
            "--validate-startup-only",
        ]
    )

    assert rc == 2
    payload = json.loads(capsys.readouterr().err)
    assert payload["authority_confirmation_receipt"]["status"] == "blocked"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["status"] == "blocked"
    assert "authority_confirmation_text_mismatch" in receipt["blockers"]
    assert receipt["opens_com_ports"] is False


def test_operator_workstation_saves_archive_confirmation_receipt_from_handoff(
    tmp_path,
    monkeypatch,
) -> None:
    root = _root()
    repository_root = Path(__file__).resolve().parents[1]
    co2, h2o = write_legacy_profile_queues(tmp_path)
    archive, _, _ = write_decision_authority_archive(tmp_path)
    receipt_path = tmp_path / "ui_archive_confirmation.json"
    info_messages: list[str] = []
    try:
        app = OperatorWorkstationApp(
            root,
            initial_settings={
                "config": str(repository_root / "configs" / "default_config.json"),
                "co2": str(co2),
                "h2o": str(h2o),
                "output": str(tmp_path),
                "authority_archive": str(archive),
                "authority_run_id": "formal_batch_001",
                "authority_device_ids": "001,002",
                "authority_operator": "operator-ui",
            },
        )
        monkeypatch.setattr(workstation_ui.messagebox, "askyesno", lambda *a, **k: True)
        monkeypatch.setattr(
            workstation_ui.filedialog,
            "asksaveasfilename",
            lambda *a, **k: str(receipt_path),
        )
        monkeypatch.setattr(
            workstation_ui.messagebox,
            "showinfo",
            lambda _title, message, **_kwargs: info_messages.append(str(message)),
        )
        monkeypatch.setattr(
            workstation_ui.messagebox,
            "showerror",
            lambda *_args, **_kwargs: pytest.fail("unexpected confirmation error"),
        )

        app.save_authority_confirmation_receipt()

        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        assert receipt["status"] == "confirmed"
        assert receipt["operator_confirmation"]["operator_name"] == "operator-ui"
        assert receipt["opens_com_ports"] is False
        assert info_messages and str(receipt_path.resolve()) in info_messages[0]
    finally:
        root.destroy()


def test_fixed_startup_preflight_blocks_invalid_config_before_tk(
    tmp_path,
    capsys,
) -> None:
    invalid = tmp_path / "invalid.json"
    invalid.write_text("{invalid", encoding="utf-8")

    rc = workstation_main(
        [
            "--config",
            str(invalid),
            "--validate-startup-only",
        ]
    )

    assert rc == 2
    payload = json.loads(capsys.readouterr().err)
    assert payload["status"] == "blocked"
    assert any(
        str(reason).startswith("runtime_config_invalid:")
        for reason in payload["blockers"]
    )


def test_operator_workstation_navigation_opens_v1_5_certificate_and_visitor_pages(
    tmp_path,
) -> None:
    root = _root()
    try:
        app = OperatorWorkstationApp(root)
        assert len(app.channel_card_vars) == 8
        app.certificate_page.registry = (
            workstation_ui.CertificateMetricsRegistry(
                tmp_path / "certificate_metrics_registry.json"
            )
        )
        app.certificate_page.refresh_records()

        app.nav_buttons["certificate"].invoke()
        root.update_idletasks()
        assert app.nav_buttons["certificate"].instate(["selected"])
        assert not app.nav_buttons["run"].instate(["selected"])
        app.certificate_page.form_vars["asset_id"].set("co2-cylinder-400ppm")
        app.certificate_page.form_vars["asset_name"].set("CO2 标准气体 400 ppm")
        app.certificate_page.form_vars["certificate_id"].set(
            "CERT-CO2-400-2026"
        )
        app.certificate_page.form_vars["certified_value"].set("399.67")
        app.certificate_page.form_vars["unit"].set("ppm")
        app.certificate_page.save_draft()
        record = app.certificate_page.registry.list_records()[0]
        assert record["calibration_input_connected"] is False
        assert record["not_real_acceptance_evidence"] is True

        app.nav_buttons["visitor"].invoke()
        root.update_idletasks()
        assert app.nav_buttons["visitor"].instate(["selected"])
        texts = [
            str(app.visitor_page.canvas.itemcget(item, "text") or "")
            for item in app.visitor_page.canvas.find_all()
            if app.visitor_page.canvas.type(item) == "text"
        ]
        joined = "\n".join(texts)
        assert "V1.5 气体分析仪智能校准中心" in joined
        assert "45 / 13 + 干气锚" in joined
        assert "0 / 8" in joined
        assert "1 Hz（校准）" in joined
        assert "A1=-- / A2=--" in joined
        assert "表值为准 / 控制器调节" in joined
        assert "49 × 49" not in joined
        assert "非真机验收证据" in joined
        assert "V2" not in joined
        assert "开始校准" not in joined
        assert app._visitor_snapshot()["opens_com_ports"] is False
        assert app._visitor_snapshot()["writes_coefficients"] is False

        app.nav_buttons["run"].invoke()
        assert app.nav_buttons["run"].instate(["selected"])
    finally:
        root.destroy()


def test_operator_workstation_summary_pages_share_one_read_only_snapshot() -> None:
    root = _root()
    try:
        app = OperatorWorkstationApp(root)

        for nav_key, page in (
            ("qc", app.qc_page),
            ("results", app.results_page),
            ("devices", app.devices_page),
            ("algorithm", app.algorithm_page),
            ("report", app.reports_page),
            ("review", app.review_page),
            ("plan", app.plan_page),
        ):
            app.nav_buttons[nav_key].invoke()
            root.update_idletasks()
            assert app.nav_buttons[nav_key].instate(["selected"])
            assert page.last_snapshot is app.current_snapshot
            assert page.readonly_widgets
            assert all(
                str(widget.cget("state")) == "disabled"
                for widget in page.readonly_widgets
            )

        shared = app.current_snapshot
        assert app.results_page.last_snapshot is shared
        assert app.reports_page.last_snapshot is shared
        assert app.review_page.last_snapshot is shared
        assert app.plan_page.last_snapshot is shared
        assert app.qc_page.last_snapshot is shared
        assert app.devices_page.last_snapshot is shared
        assert app.algorithm_page.last_snapshot is shared
        assert app.visitor_page.snapshot is shared
        report_text = "\n".join(
            widget.get("1.0", "end")
            for widget in app.reports_page.readonly_widgets
        )
        assert "mature_v1_5_runner_artifacts" in report_text
        assert "ok / skipped / missing / error" in report_text
        assert "正式签发状态：not_evaluated" in report_text
        assert "本页不执行导出、签发或批准" in report_text
        assert app.results_page.metric_vars[1].get() == "45"
        assert app.results_page.metric_vars[2].get() == "13"
        assert app.plan_page.metric_vars[1].get() == "legacy_ratio_production"
        assert app.plan_page.metric_vars[2].get() == "58"
        assert app.qc_page.metric_vars[1].get() == "pass"
        assert app.qc_page.metric_vars[2].get() == "pending"
        assert app.qc_page.metric_vars[3].get() == "not_evaluated"
        qc_text = "\n".join(
            widget.get("1.0", "end")
            for widget in app.qc_page.readonly_widgets
        )
        assert "execution_rows" in qc_text
        assert "mature_v1_5_runner_artifacts" in qc_text
        assert "threshold_profile_hash" in qc_text
        assert "UI 可编辑：false" in qc_text
        assert app.devices_page.metric_vars[0].get() == "simulation_only"
        assert app.devices_page.metric_vars[1].get() == "8"
        assert app.devices_page.metric_vars[2].get() == "0"
        assert app.devices_page.metric_vars[3].get() == "8"
        review_text = "\n".join(
            widget.get("1.0", "end")
            for widget in app.review_page.readonly_widgets
        )
        simulation_status = app.current_snapshot["decision_model"]["decisions"][
            "start_simulation"
        ]["status"]
        assert simulation_status in {"allowed", "blocked"}
        expected_simulation_label = (
            "允许" if simulation_status == "allowed" else "锁定"
        )
        assert f"仿真演练启动：{expected_simulation_label}" in review_text
        assert "真实执行启动：锁定" in review_text
        assert "受控系数写入：锁定" in review_text
        assert "正式证书签发：锁定" in review_text
        device_text = "\n".join(
            widget.get("1.0", "end")
            for widget in app.devices_page.readonly_widgets
        )
        assert "八个槽位来自工作站通道合同" in device_text
        assert "六个槽位" not in device_text
        device_labels = "\n".join(_texts(app.devices_page))
        assert "mature_v1_v1_5_artifacts_only" in device_text
        assert "SENCO7_SENCO8_neutral" in device_text
        assert "1 Hz 仅指 calibration_upload_timebase" in device_text
        assert "AVERAGE1=--、AVERAGE2=-- 为独立参数" in device_text
        assert "V2 仿真预设、故障注入和第二套设备状态源不进入" in (
            device_text
        )
        assert app.algorithm_page.metric_vars[0].get() == (
            "locked_production_default"
        )
        assert app.algorithm_page.metric_vars[1].get() == (
            "legacy_ratio_production"
        )
        assert app.algorithm_page.metric_vars[2].get() == "legacy_ratio_R"
        assert app.algorithm_page.metric_vars[3].get() == "1"
        algorithm_text = "\n".join(
            widget.get("1.0", "end")
            for widget in app.algorithm_page.readonly_widgets
        )
        assert "压力真值：数字压力计" in algorithm_text
        assert "SENCO9拟合对象：分析仪内部压力相对数字压力计的偏差" in (
            algorithm_text
        )
        assert "设备身份与健康摘要" in device_labels
        assert "生产算法与候选边界" in "\n".join(_texts(app.algorithm_page))
        assert "开始真实校准" not in device_text
        assert "切换生产算法" not in "\n".join(_texts(app.algorithm_page))
        assert app.review_page.metric_vars[1].get() == "pass"
        assert shared["review"]["approval_actions_available"] is False
        assert shared["review"]["coefficient_write_actions_available"] is False
        assert shared["opens_com_ports"] is False
        assert shared["writes_coefficients"] is False
    finally:
        root.destroy()


def test_operator_workstation_i18n_defaults_to_chinese_with_english_fallback() -> None:
    assert _t("title") == "V1.5 气体分析仪校准工作站"
    assert _t("title", locale="en_US") == "V1.5 Gas Analyzer Calibration Workstation"
    assert _t("handoff.preflight.run") == "离线预检（不执行）"
    assert _t("handoff.preflight.run", locale="en_US") == (
        "Offline preflight (no execution)"
    )
    assert _t("handoff.preflight.save") == "保存预检回执"
    assert _t("handoff.preflight.save", locale="en_US") == (
        "Save preflight receipt"
    )
    assert _t("route.zero", locale="en_US") == "CO₂ 零气锚点"
    assert _t("evidence.pressure_gauge_short") == "表"
    assert OperatorWorkstationApp._format_pressure_delta(
        {"controller_minus_reference_hpa": 0.46}
    ) == "+0.5 hPa"
    assert OperatorWorkstationApp._format_pressure_delta({}) == "-- hPa"
    assert OperatorWorkstationApp._format_pressure_chain_status(
        {"status": "controller_feedback_missing"}
    ) == "压力链：未就绪｜压力控制器无有效反馈"
    assert OperatorWorkstationApp._format_pressure_chain_status(
        {"status": "fresh_coincident_observation"},
        locale="en_US",
    ) == "Pressure chain: readback timing ready"


def test_operator_workstation_start_button_wires_only_the_dry_run_executor(
    tmp_path,
    monkeypatch,
) -> None:
    root = _root()
    calls: list[dict] = []
    info_messages: list[str] = []
    plan = {
        "schema": "v1_5_operator_workstation_dry_run_v1",
        "overall_status": "ready_for_v1_5_dry_run",
        "blockers": [],
        "warnings": ["certificate_registry_not_configured_non_blocking"],
        "point_counts": {"co2": 45, "h2o": 13},
        "opens_com_ports": False,
        "writes_coefficients": False,
    }

    def executor(payload):
        calls.append(dict(payload))
        return {
            **dict(payload),
            "overall_status": "pass",
            "execution_blockers": [],
            "route_results": [
                {"route_kind": "co2", "status": "pass", "dry_run_points": 45},
                {"route_kind": "h2o", "status": "pass", "dry_run_points": 13},
            ],
        }

    try:
        app = OperatorWorkstationApp(root, executor=executor)
        app.settings["output"].set(str(tmp_path))
        monkeypatch.setattr(app, "_plan", lambda: dict(plan))
        monkeypatch.setattr(
            workstation_ui.messagebox,
            "showinfo",
            lambda _title, message, **_kwargs: info_messages.append(str(message)),
        )
        app.start_dry_run()
        deadline = time.monotonic() + 3.0
        while app.last_result is None and time.monotonic() < deadline:
            root.update()
            time.sleep(0.01)

        assert len(calls) == 1
        assert calls[0]["opens_com_ports"] is False
        assert calls[0]["writes_coefficients"] is False
        assert app.last_result is not None
        assert app.last_result["overall_status"] == "pass"
        assert app.status_var.get() == "系统状态：45/13 dry-run 已通过"
        assert app.start_button.instate(["!disabled"])
        assert info_messages == ["成熟 V1.5 路径演练通过：CO₂ 45 点，H₂O 13 点。"]
        assert app.current_snapshot["overall_status"] == "pass"
        assert app.current_snapshot["reports"]["present_count"] == 2
        assert app.results_page.last_snapshot is app.current_snapshot
        assert app.reports_page.last_snapshot is app.current_snapshot
        assert app.review_page.last_snapshot is app.current_snapshot
        assert app.plan_page.last_snapshot is app.current_snapshot
        assert app.qc_page.last_snapshot is app.current_snapshot
        assert app.devices_page.last_snapshot is app.current_snapshot
        assert app.algorithm_page.last_snapshot is app.current_snapshot
        assert app.current_snapshot["plan"]["status"] == "executed_dry_run"
        assert app.current_snapshot["qc"]["overall_status"] == "dry_run_pass"
        assert app.qc_page.metric_vars[2].get() == "pass"
        assert app.qc_page.metric_vars[3].get() == "not_evaluated"
        assert app.devices_page.metric_vars[2].get() == "0"
        assert app.algorithm_page.metric_vars[0].get() == (
            "locked_production_default"
        )
    finally:
        root.destroy()
