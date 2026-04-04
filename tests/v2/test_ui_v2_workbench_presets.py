from __future__ import annotations

import json
from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.i18n import t

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import build_fake_facade


def test_workbench_presets_update_state_history_filters_and_snapshot_compare(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)

    facade.execute_device_workbench_action("analyzer", "run_preset", preset_id="mode2_active_read", analyzer_index=0)
    facade.execute_device_workbench_action("analyzer", "run_preset", preset_id="partial_frame", analyzer_index=0)
    facade.execute_device_workbench_action(
        "workbench",
        "set_history_filters",
        device_filter="analyzer",
        result_filter="fault_injection",
    )

    snapshot = facade.get_device_workbench_snapshot()
    history = snapshot["history"]
    compare_options = snapshot["workbench"]["snapshot_compare"]["options"]

    assert snapshot["analyzer"]["panel_status"]["mode"] == 2
    assert snapshot["analyzer"]["injection_state"]["mode2_stream"] == "partial_frame"
    assert any(item["action"] == "run_preset" for item in history["all_items"])
    assert history["items"]
    assert all(item["device"] == "analyzer" for item in history["items"])
    assert all(item["is_fault_injection"] for item in history["items"])
    assert history["filters"]["device"] == "analyzer"
    assert history["filters"]["result"] == "fault_injection"
    assert len(compare_options) >= 2

    facade.execute_device_workbench_action(
        "workbench",
        "set_snapshot_compare",
        left_sequence=compare_options[-1]["sequence"],
        right_sequence=compare_options[0]["sequence"],
    )
    compare_snapshot = facade.get_device_workbench_snapshot()["workbench"]["snapshot_compare"]

    assert compare_snapshot["available"] is True
    assert compare_snapshot["summary"]
    assert compare_snapshot["details_text"]


def test_workbench_presets_are_chinese_first_and_enter_evidence(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)

    preset_labels = [
        item["label"]
        for item in facade.get_device_workbench_snapshot()["pressure_gauge"]["presets"]
    ]
    assert t("pages.devices.workbench.preset.pressure_gauge.wrong_unit.label") in preset_labels

    facade.execute_device_workbench_action("pressure_gauge", "run_preset", preset_id="wrong_unit")
    result = facade.execute_device_workbench_action(
        "workbench",
        "generate_diagnostic_evidence",
        current_device="pressure_gauge",
        current_action="run_preset",
    )

    report_payload = json.loads(
        (Path(facade.result_store.run_dir) / "workbench_action_report.json").read_text(encoding="utf-8")
    )

    assert result["ok"] is True
    assert report_payload["risk_level"] == "medium"
    assert report_payload["has_fault_injection"] is True
    assert report_payload["reference_quality_summary"]
    assert report_payload["route_relay_summary"]
    assert any(item["action"] == "run_preset" for item in report_payload["history"])


def test_workbench_preset_center_groups_frequent_and_recent_are_exposed(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)

    facade.execute_device_workbench_action("analyzer", "run_preset", preset_id="mode2_active_read", analyzer_index=0)
    facade.execute_device_workbench_action("pressure_gauge", "run_preset", preset_id="wrong_unit")

    snapshot = facade.get_device_workbench_snapshot()
    preset_center = snapshot["workbench"]["preset_center"]
    group_ids = {item["id"] for item in preset_center["groups"]}
    recent = preset_center["recent_presets"]
    pressure_group = next(item for item in preset_center["groups"] if item["id"] == "pressure")

    assert group_ids == {"analyzer", "pace", "grz", "chamber", "relay", "thermometer", "pressure"}
    assert pressure_group["frequent_presets"]
    assert recent
    assert recent[0]["device_kind"] == "pressure_gauge"
    assert recent[0]["label"] == t("pages.devices.workbench.preset.pressure_gauge.wrong_unit.label")


def test_workbench_history_detail_links_snapshot_and_evidence(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)

    facade.execute_device_workbench_action("relay", "run_preset", preset_id="route_h2o", relay_name="relay", channel=1)
    facade.execute_device_workbench_action("pressure_gauge", "run_preset", preset_id="wrong_unit")
    facade.execute_device_workbench_action(
        "workbench",
        "generate_diagnostic_evidence",
        current_device="pressure_gauge",
        current_action="run_preset",
    )

    snapshot = facade.get_device_workbench_snapshot()
    detail = snapshot["history"]["detail"]

    assert detail["related_snapshot"]["available"] is True
    assert detail["related_evidence"]["available"] is True
    assert snapshot["engineer_summary"]["sections"]
