from __future__ import annotations

import hashlib
import json
import tkinter as tk
from pathlib import Path
from tkinter import ttk

import pytest

from gas_calibrator.v1_5.ui import operator_workstation_app as workstation_ui
from gas_calibrator.v1_5.ui.operator_workstation_app import OperatorWorkstationApp
from gas_calibrator.v1_5.ui.page_i18n import translator_for
from gas_calibrator.v1_5.ui.pages.site_profile_page import SiteProfilePage


def _root() -> tk.Tk:
    try:
        root = tk.Tk()
    except tk.TclError as exc:  # pragma: no cover - environment guard
        pytest.skip(f"Tk unavailable: {exc}")
    root.withdraw()
    return root


def _write_inventory(tmp_path: Path) -> Path:
    path = tmp_path / "runtime_serial_port_inventory.json"
    path.write_text(
        json.dumps(
            {
                "schema": "v1_5_runtime_serial_port_inventory_v1",
                "opens_com_ports": False,
                "sends_device_commands": False,
                "ports": [{"port": f"COM{index}"} for index in range(35, 43)],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return path


def _complete_four_connected_two_powered(page: SiteProfilePage) -> None:
    for index, row in enumerate(page.profile["candidate_analyzers"], start=1):
        row["connected"] = index <= 4
        row["powered"] = index <= 2
        row["operator_confirmed"] = index <= 4
        row["ga_label"] = f"GA{index:02d}" if index <= 4 else ""
        if index <= 2:
            row.update(
                {
                    "protocol_device_id": f"{index:03d}",
                    "sn_code": f"012607{index:02d}",
                    "algorithm": "legacy_ratio",
                    "check_capable": False,
                    "check_required": False,
                    "runtime_evidence": {
                        "ftd_hz": 1.0,
                        "average1": "AVERAGE1",
                        "average2": "AVERAGE2",
                        "filter": "operator_reviewed",
                    },
                }
            )


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def test_site_profile_page_saves_hash_bound_four_two_readonly_inputs(
    tmp_path: Path,
) -> None:
    root = _root()
    try:
        inventory = _write_inventory(tmp_path)
        page = SiteProfilePage(
            root,
            profile_path=tmp_path / "site_profile.json",
        )
        page.pack(fill="both", expand=True)
        page.create_from_inventory(inventory)

        assert len(page.profile["candidate_analyzers"]) == 8
        assert page.metric_vars[0].get() == "4"
        assert page.metric_vars[1].get() == "2"
        assert page.validation == {}
        assert page.profile["opens_com_ports"] is False
        assert page.profile["sends_device_commands"] is False
        assert page.profile["writes_coefficients"] is False

        _complete_four_connected_two_powered(page)
        unconfirmed = page.validate_profile(show_dialog=False)
        assert unconfirmed["ready_for_readonly_packet_build"] is False
        assert "current_site_confirmation_missing" in unconfirmed["reasons"]

        page.confirmation_vars["operator_name"].set("现场操作员")
        page.confirmation_vars["observation_basis"].set("逐台检查线缆与电源指示灯")
        assert page.confirm_current_state(show_dialog=False) is True
        validation = page.validation
        assert validation["ready_for_readonly_packet_build"] is True
        assert validation["mapped_connected_count"] == 4
        assert validation["mapped_powered_count"] == 2
        assert page.profile["current_site_confirmation"]["connected_ports"] == [
            "COM35",
            "COM36",
            "COM37",
            "COM38",
        ]
        assert page.profile["current_site_confirmation"]["powered_ports"] == [
            "COM35",
            "COM36",
        ]
        assert (
            len(
                page.profile["current_site_confirmation"][
                    "candidate_state_sha256"
                ]
            )
            == 64
        )
        assert page.tree.item("COM35", "values")[-1] == "就绪"
        assert page.tree.item("COM37", "values")[-1] == "就绪"
        assert page.tree.item("COM42", "values")[-1] == "未用于本次"

        outputs = page.save_profile(tmp_path / "site_profile.json")
        saved_profile = _load_json(outputs["profile"])
        reviewed = _load_json(outputs["reviewed_ports"])
        active = _load_json(outputs["active_analyzers"])
        expected_sha = hashlib.sha256(outputs["profile"].read_bytes()).hexdigest()

        assert saved_profile["profile_status"] == "ready_for_readonly_packet_build"
        assert len(reviewed["reviewed_ports"]) == 4
        assert len(active["active_analyzers"]) == 2
        assert reviewed["source_site_profile_sha256"] == expected_sha
        assert active["source_site_profile_sha256"] == expected_sha
        assert reviewed["blocked_reasons"] == []
        assert active["blocked_reasons"] == []
    finally:
        root.destroy()


def test_site_profile_page_invalidates_confirmation_after_row_edit(
    tmp_path: Path,
) -> None:
    root = _root()
    try:
        inventory = _write_inventory(tmp_path)
        page = SiteProfilePage(
            root,
            profile_path=tmp_path / "site_profile.json",
        )
        page.create_from_inventory(inventory)
        _complete_four_connected_two_powered(page)
        page.confirmation_vars["operator_name"].set("现场操作员")
        page.confirmation_vars["observation_basis"].set("逐台检查线缆与电源指示灯")
        assert page.confirm_current_state(show_dialog=False) is True

        page.tree.selection_set("COM35")
        page._on_selected()
        page.form_vars["average1"].set("AVERAGE1-EDITED")
        page.apply_selected_row()

        assert (
            page.profile["current_site_confirmation"]["status"]
            == "stale_after_mapping_edit"
        )
        assert page.validation["ready_for_readonly_packet_build"] is False
        assert (
            "current_site_confirmation_not_confirmed"
            in page.validation["reasons"]
        )
        assert "原确认失效" in page.confirmation_status_var.get()
    finally:
        root.destroy()


def test_site_profile_page_clears_derived_lists_when_mapping_is_blocked(
    tmp_path: Path,
) -> None:
    root = _root()
    try:
        inventory = _write_inventory(tmp_path)
        page = SiteProfilePage(
            root,
            profile_path=tmp_path / "site_profile.json",
        )
        page.create_from_inventory(inventory)
        outputs = page.save_profile()

        saved_profile = _load_json(outputs["profile"])
        validation = _load_json(outputs["validation"])
        reviewed = _load_json(outputs["reviewed_ports"])
        active = _load_json(outputs["active_analyzers"])

        assert saved_profile["profile_status"] == "review_required"
        assert validation["ready_for_readonly_packet_build"] is False
        assert validation["reasons"]
        assert reviewed["reviewed_ports"] == []
        assert active["active_analyzers"] == []
        assert reviewed["blocked_reasons"] == validation["reasons"]
        assert active["blocked_reasons"] == validation["reasons"]
        display_reasons = page.reason_text.get("1.0", "end")
        assert "报告接入 4 台，当前只确认 0 台" in display_reasons
        assert "报告通电 2 台，当前只确认 0 台" in display_reasons
        assert "connected_count_expected" not in display_reasons
        assert "powered_count_expected" not in display_reasons
    finally:
        root.destroy()


def test_site_profile_page_marks_historical_identity_as_pending_current_confirmation(
    tmp_path: Path,
) -> None:
    root = _root()
    try:
        inventory = _write_inventory(tmp_path)
        page = SiteProfilePage(
            root,
            profile_path=tmp_path / "site_profile.json",
        )
        page.create_from_inventory(inventory)
        page.profile["candidate_analyzers"][0].update(
            {
                "ga_label": "GA01",
                "protocol_device_id": "004",
                "sn_code": "01260604",
                "algorithm": "legacy_ratio",
                "identity_evidence": {
                    "scope": "historical_identity_prefill_only",
                },
            }
        )
        page.profile["historical_identity_prefill"] = {
            "applied_count": 1,
        }
        source = tmp_path / "historical_profile.json"
        source.write_text(
            json.dumps(page.profile, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        page.load_profile(source)

        assert page.tree.item("COM35", "values")[-1] == "历史身份待确认"
        assert "其中 1 台为历史身份预填" in page.status_var.get()
        assert page.profile["candidate_analyzers"][0]["connected"] is None
        assert page.profile["candidate_analyzers"][0]["powered"] is None
    finally:
        root.destroy()


def test_operator_workstation_exposes_site_mapping_without_replacing_devices_page(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    defaults = workstation_ui._default_paths()
    defaults["output"] = str(tmp_path)
    monkeypatch.setattr(workstation_ui, "_default_paths", lambda: defaults)
    root = _root()
    try:
        app = OperatorWorkstationApp(root)
        root.geometry("1920x1080+0+0")
        root.deiconify()
        app.nav_buttons["site"].invoke()
        root.update_idletasks()
        root.update()

        assert app.nav_buttons["site"].instate(["selected"])
        assert app.site_profile_page.last_snapshot is app.current_snapshot
        assert app.devices_page is not app.site_profile_page
        assert app.pages["devices"] is app.devices_page
        assert app.pages["site"] is app.site_profile_page
        assert app.site_profile_page.page_scaffold.canvas.winfo_width() >= 1400
        assert (
            not app.site_profile_page.page_scaffold.has_overflow()
            or app.site_profile_page.page_scaffold.is_scrollbar_visible()
        )
        style = ttk.Style(root)
        assert style.lookup("Site.TEntry", "fieldbackground") == "#102131"
        assert style.lookup("Site.TCheckbutton", "background") == "#0c1927"
        assert app.current_snapshot["opens_com_ports"] is False
        assert app.current_snapshot["writes_coefficients"] is False
    finally:
        root.destroy()


def test_site_profile_page_i18n_is_chinese_first_with_english_fallback() -> None:
    zh = translator_for("zh_CN")
    en = translator_for("en_US")

    assert zh("pages.site_profile.title") == "现场设备配置与只读初始化准备"
    assert zh("pages.site_profile.actions.save") == "保存配置与清单"
    assert zh("pages.site_profile.value.historical_prefill") == "历史身份待确认"
    assert zh("pages.site_profile.confirmation.action") == "确认并绑定当前4/2映射"
    assert "历史6台身份记录不能证明" in zh(
        "pages.site_profile.reason.confirmation_missing"
    )
    assert en("pages.site_profile.title") == (
        "Site Device Mapping and Read-only Initialization"
    )
    assert en("pages.site_profile.actions.save") == "Save Profile and Lists"
    assert en("pages.site_profile.confirmation.action") == (
        "Confirm and Bind Current 4/2 Mapping"
    )
