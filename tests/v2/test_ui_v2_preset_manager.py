from __future__ import annotations

import json
from pathlib import Path
import sys

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import build_fake_facade


def _create_pressure_custom_preset(facade, *, label: str, description: str) -> dict:
    result = facade.execute_device_workbench_action(
        "workbench",
        "save_custom_preset",
        group_id="pressure",
        label=label,
        description=description,
        analyzer_index=1,
        pressure_hpa=955.0,
        relay_name="relay",
        channel=2,
        steps=[
            {"device_kind": "pressure_gauge", "preset_id": "wrong_unit"},
            {"device_kind": "relay", "preset_id": "route_h2o"},
        ],
    )
    assert result["ok"] is True
    return dict(result["custom_preset"])


def test_preset_manager_can_duplicate_export_import_with_metadata_and_history_chain(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path / "source")
    custom_preset = _create_pressure_custom_preset(
        facade,
        label="pressure_chain",
        description="simulation-only pressure preset",
    )

    duplicate_result = facade.execute_device_workbench_action(
        "workbench",
        "duplicate_preset",
        device_kind="analyzer",
        preset_id="mode2_active_read",
    )
    assert duplicate_result["ok"] is True
    assert duplicate_result["custom_preset"]["is_custom"] is True

    export_result = facade.execute_device_workbench_action(
        "workbench",
        "export_preset_bundle",
        scope="group",
        group_id="pressure",
    )
    bundle = json.loads(str(export_result["bundle_text"] or "{}"))

    assert export_result["ok"] is True
    assert bundle["schema"] == "simulation_preset_bundle_v1"
    assert bundle["schema_version"] == 2
    assert bundle["preset_schema_version"] == "preset_definition_v2"
    assert bundle["simulation_only"] is True
    assert bundle["evidence_source"] == "simulated"
    assert bundle["not_real_acceptance_evidence"] is True
    assert bundle["conflict_policy"] == "rename"
    assert bundle["bundle_profile"] == "simulation_only_local_exchange"
    assert str(bundle.get("conflict_policy_summary") or "").strip()
    assert "sharing_scope" in list(bundle.get("sharing_reserved_fields") or [])
    assert "sharing_scope" in str(bundle.get("sharing_reserved_fields_summary") or "")
    assert "simulation_only_local_exchange" in str(bundle.get("bundle_profile_summary") or "")
    assert bundle["sharing_interface"]["supports_import_export_only"] is True
    assert "simulation_preset_bundle_v1" in str(bundle.get("bundle_format_summary") or "")
    assert "sharing_scope" in str(bundle.get("sharing_ready_summary") or "")
    assert bundle["preset_count"] >= 4
    assert len({str(item.get("id") or "") for item in bundle["presets"]}) == bundle["preset_count"]
    exported_custom = next(item for item in bundle["presets"] if str(item.get("origin") or "") == "local_editor")
    assert exported_custom["schema_version"] == "preset_definition_v2"
    assert exported_custom["preset_version"] >= 1
    assert exported_custom["origin"] == "local_editor"
    assert exported_custom["fake_capabilities"]
    assert str(exported_custom.get("fake_capability_summary") or "").strip()

    imported_facade = build_fake_facade(tmp_path / "imported")
    import_result = imported_facade.execute_device_workbench_action(
        "workbench",
        "import_preset_bundle",
        bundle_text=export_result["bundle_text"],
    )
    imported_custom_presets = [dict(item) for item in list(import_result.get("custom_presets", []) or [])]

    assert import_result["ok"] is True
    assert import_result["imported_count"] == bundle["preset_count"]
    assert import_result["created_count"] == bundle["preset_count"]
    assert import_result["overwritten_count"] == 0
    assert import_result["conflict_policy"] == "rename"
    assert import_result["renamed_count"] == 0
    assert str(import_result.get("conflict_policy_summary") or "").strip()
    assert "sharing_scope" in str(import_result.get("sharing_reserved_fields_summary") or "")
    assert "simulation_only_local_exchange" in str(import_result.get("bundle_profile_summary") or "")
    assert "重命名" in str(import_result.get("conflict_summary") or "")
    assert "simulation_preset_bundle_v1" in str(import_result.get("bundle_format_summary") or "")
    assert len(imported_custom_presets) == bundle["preset_count"]
    assert all(item["source_kind"] == "custom" for item in imported_custom_presets)
    assert all(item["schema_version"] == "preset_definition_v2" for item in imported_custom_presets)
    assert all(int(item["preset_version"]) >= 1 for item in imported_custom_presets)
    assert all(str(item.get("origin") or "") == "import_bundle" for item in imported_custom_presets)
    assert all(str(item.get("imported_from") or "").strip() for item in imported_custom_presets)
    assert all(str(item.get("sharing_scope") or "") == "local_reserved" for item in imported_custom_presets)
    assert all(str(item.get("metadata_summary") or "").strip() for item in imported_custom_presets)
    assert all(str(item.get("fake_capability_summary") or "").strip() for item in imported_custom_presets)

    imported_row = next(item for item in imported_custom_presets if str(item.get("label") or "") == "pressure_chain")
    run_result = imported_facade.execute_device_workbench_action(
        str(imported_row["device_kind"] or ""),
        "run_preset",
        preset_id=str(imported_row["id"] or ""),
    )
    snapshot = imported_facade.get_device_workbench_snapshot()
    manager = snapshot["workbench"]["preset_center"]["manager"]
    report_payload = json.loads(
        (Path(imported_facade.result_store.run_dir) / "workbench_action_report.json").read_text(encoding="utf-8")
    )

    assert run_result["ok"] is True
    assert snapshot["history"]["all_items"][0]["params"]["custom_preset_id"] == imported_row["id"]
    assert snapshot["history"]["all_items"][0]["params"]["preset_source"] == "custom"
    assert snapshot["history"]["all_items"][0]["params"]["preset_fake_capabilities"]
    assert str(snapshot["history"]["all_items"][0]["params"]["preset_fake_capability_summary"] or "").strip()
    assert snapshot["workbench"]["snapshot_compare"]["options"]
    assert str(snapshot["workbench"]["preset_center"]["manager"]["selected_preset_capability_summary"] or "").strip()
    assert manager["directory_index"]["builtin"]["count"] > 0
    assert manager["directory_index"]["imported"]["count"] >= bundle["preset_count"]
    assert manager["simulation_only"] is True
    assert manager["evidence_source"] == "simulated"
    assert manager["not_real_acceptance_evidence"] is True
    assert manager["conflict_policy"] == "rename"
    assert manager["bundle_profile"] == "simulation_only_local_exchange"
    assert "sharing_scope" in list(manager.get("sharing_reserved_fields") or [])
    assert str(manager.get("conflict_policy_summary") or "").strip()
    assert "simulation_preset_bundle_v1" in str(manager.get("bundle_format_summary") or "")
    assert str(manager.get("conflict_strategy_summary") or "").strip()
    assert "sharing_scope" in str(manager.get("sharing_reserved_fields_summary") or "")
    assert "simulation_only_local_exchange" in str(manager.get("bundle_profile_summary") or "")
    assert "sharing_scope" in str(manager.get("sharing_ready_summary") or "")
    assert str(manager.get("selected_preset_metadata_summary") or "").strip()
    assert str(manager.get("selected_preset_capability_summary") or "").strip()
    assert "导入" in str(manager["directory_summary"] or "")
    assert any(
        dict(item.get("params", {}) or {}).get("custom_preset_id") == imported_row["id"]
        for item in report_payload["history"]
    )
    assert any(
        str(dict(item.get("params", {}) or {}).get("preset_fake_capability_summary") or "").strip()
        for item in report_payload["history"]
    )
    assert any(
        item["type"] == "workbench"
        for item in imported_facade.build_results_snapshot()["review_center"]["evidence_items"]
    )


def test_preset_manager_handles_import_conflicts_with_rename_and_overwrite(tmp_path: Path) -> None:
    source_facade = build_fake_facade(tmp_path / "source_bundle")
    source_preset = _create_pressure_custom_preset(
        source_facade,
        label="pressure_chain",
        description="source preset",
    )
    export_result = source_facade.execute_device_workbench_action(
        "workbench",
        "export_preset_bundle",
        scope="selected",
        device_kind=str(source_preset["device_kind"] or ""),
        preset_id=str(source_preset["id"] or ""),
    )

    rename_facade = build_fake_facade(tmp_path / "rename_target")
    existing_rename = _create_pressure_custom_preset(
        rename_facade,
        label="pressure_chain",
        description="rename target baseline",
    )
    rename_result = rename_facade.execute_device_workbench_action(
        "workbench",
        "import_preset_bundle",
        bundle_text=export_result["bundle_text"],
        conflict_policy="rename",
    )
    rename_snapshot = rename_facade.get_device_workbench_snapshot()["workbench"]["preset_center"]["custom_presets"]
    rename_labels = [str(item.get("label") or "") for item in rename_snapshot]
    renamed_row = next(item for item in rename_result["custom_presets"] if str(item.get("label") or "") != "pressure_chain")

    assert rename_result["ok"] is True
    assert rename_result["conflict_policy"] == "rename"
    assert rename_result["imported_count"] == 1
    assert rename_result["created_count"] == 1
    assert rename_result["renamed_count"] == 1
    assert rename_result["overwritten_count"] == 0
    assert "重命名 1" in str(rename_result.get("conflict_summary") or "")
    assert str(renamed_row["id"] or "") != str(existing_rename["id"] or "")
    assert "pressure_chain" in rename_labels
    assert str(renamed_row["label"] or "") in rename_labels
    assert str(renamed_row["label"] or "") != "pressure_chain"
    assert rename_facade.get_preferences()["workbench"]["preset_preferences"]["import_conflict_policy"] == "rename"

    overwrite_facade = build_fake_facade(tmp_path / "overwrite_target")
    existing_overwrite = _create_pressure_custom_preset(
        overwrite_facade,
        label="pressure_chain",
        description="overwrite target baseline",
    )
    overwrite_result = overwrite_facade.execute_device_workbench_action(
        "workbench",
        "import_preset_bundle",
        bundle_text=export_result["bundle_text"],
        conflict_policy="overwrite",
    )
    overwrite_snapshot = overwrite_facade.get_device_workbench_snapshot()["workbench"]["preset_center"]["custom_presets"]
    overwritten_row = next(item for item in overwrite_snapshot if str(item.get("id") or "") == str(existing_overwrite["id"] or ""))

    assert overwrite_result["ok"] is True
    assert overwrite_result["conflict_policy"] == "overwrite"
    assert overwrite_result["imported_count"] == 1
    assert overwrite_result["created_count"] == 0
    assert overwrite_result["renamed_count"] == 0
    assert overwrite_result["overwritten_count"] == 1
    assert "覆盖 1" in str(overwrite_result.get("conflict_summary") or "")
    assert overwritten_row["description"] == "source preset"
    assert overwritten_row["origin"] == "import_bundle"
    assert overwritten_row["schema_version"] == "preset_definition_v2"
    assert int(overwritten_row["preset_version"]) >= 2
    assert str(overwritten_row.get("imported_from") or "").strip()
    assert overwrite_facade.get_preferences()["workbench"]["preset_preferences"]["import_conflict_policy"] == "overwrite"


def test_preset_manager_rejects_invalid_bundle_without_overwriting_state(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    before = list(facade.get_device_workbench_snapshot()["workbench"]["preset_center"]["custom_presets"])

    invalid_bundle = json.dumps(
        {
            "schema": "wrong_schema",
            "schema_version": 99,
            "simulation_only": False,
            "presets": [
                {
                    "id": "custom_pressure_01",
                    "label": "bad_bundle",
                    "group_id": "pressure",
                    "device_kind": "pressure_gauge",
                    "parameters": {"pressure_hpa": 900},
                    "steps": [{"device_kind": "pressure_gauge", "preset_id": "wrong_unit"}],
                }
            ],
        },
        ensure_ascii=False,
        indent=2,
    )

    result = facade.execute_device_workbench_action(
        "workbench",
        "import_preset_bundle",
        bundle_text=invalid_bundle,
    )
    after = list(facade.get_device_workbench_snapshot()["workbench"]["preset_center"]["custom_presets"])

    assert result["ok"] is False
    assert "schema" in str(result.get("message") or "").lower()
    assert after == before
