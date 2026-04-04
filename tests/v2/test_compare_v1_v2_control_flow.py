from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from gas_calibrator.v2.scripts import compare_v1_v2_control_flow


def _write_trace(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_points_json(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"points": rows}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def test_build_control_flow_report_summarizes_presence_counts_and_key_actions(tmp_path: Path) -> None:
    v1_trace = tmp_path / "v1.jsonl"
    v2_trace = tmp_path / "v2.jsonl"
    _write_trace(
        v1_trace,
        [
            '{"route":"co2","action":"route_baseline","point_tag":"co2_groupa_400ppm_1000hpa","result":"ok"}',
            '{"route":"co2","action":"set_pressure","point_tag":"co2_groupa_400ppm_1000hpa","result":"ok"}',
            '{"route":"co2","action":"sample_end","point_tag":"co2_groupa_400ppm_1000hpa","result":"ok","actual":{"sample_count":3}}',
            '{"route":"h2o","action":"set_vent","point_tag":"h2o_20c_50rh_1000hpa","result":"ok"}',
        ],
    )
    _write_trace(
        v2_trace,
        [
            '{"route":"co2","action":"set_pressure","point_tag":"co2_groupa_400ppm_1000hpa","result":"ok"}',
            '{"route":"co2","action":"route_baseline","point_tag":"co2_groupa_400ppm_1000hpa","result":"ok"}',
            '{"route":"co2","action":"sample_end","point_tag":"co2_groupa_400ppm_1000hpa","result":"ok","actual":{"sample_count":2}}',
        ],
    )

    report = compare_v1_v2_control_flow.build_control_flow_report(
        v1_trace_path=v1_trace,
        v2_trace_path=v2_trace,
        metadata={
            "validation_profile": "skip0_co2_only_replacement",
            "route_mode": "co2_only",
            "skip_co2_ppm": [0],
            "v1": {"ok": True},
            "v2": {"ok": True},
        },
    )

    assert report["presence"]["matches"] is True
    assert report["sample_count"]["matches"] is False
    assert report["sample_count"]["mismatches"][0]["point_tag"] == "co2_groupa_400ppm_1000hpa"
    assert report["route_sequence"]["matches"] is False
    assert report["key_actions"]["vent"]["matches"] is False
    assert report["key_actions"]["pressure"]["matches"] is True
    assert report["replacement_validation"]["only_in_v1"] == []
    assert report["replacement_validation"]["only_in_v2"] == []
    assert report["replacement_validation"]["sample_count_matches"] is False
    assert report["replacement_validation"]["route_action_order_matches"] is False
    assert report["replacement_validation"]["route_action_order_differences"]
    assert (
        report["replacement_validation"]["key_action_group_registry"]
        == "gas_calibrator.v2.scripts.route_trace_diff.KEY_ACTION_GROUPS"
    )
    assert "current main replacement-validation route" in report["validation_scope"]["summary"]
    assert "True 0 ppm behavior equivalence." in report["validation_scope"]["does_not_prove"]
    assert report["compare_status"] == compare_v1_v2_control_flow.COMPARE_STATUS_MISMATCH
    assert report["overall_match"] is False


def test_apply_runtime_overrides_relocates_old_repo_absolute_paths(monkeypatch, tmp_path: Path) -> None:
    old_base = tmp_path / "old_repo"
    current_base = tmp_path / "current_repo"
    old_points = old_base / "src" / "gas_calibrator" / "v2" / "configs" / "points.json"
    old_logs_dir = old_base / "src" / "gas_calibrator" / "v2" / "logs"
    old_database = old_base / "src" / "gas_calibrator" / "v2" / "storage" / "compare.sqlite3"
    old_model_source = old_base / "src" / "gas_calibrator" / "v2" / "output" / "v1_v2_compare" / "offline.csv"
    old_tuning = old_base / "configs" / "user_tuning.json"

    for path in (old_points, old_database, old_model_source, old_tuning):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}", encoding="utf-8")
    old_logs_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(compare_v1_v2_control_flow, "PROJECT_ROOT", current_base)
    runtime_cfg = compare_v1_v2_control_flow._apply_runtime_overrides(
        {
            "_base_dir": str(old_base),
            "paths": {
                "points_excel": str(old_points),
                "output_dir": str(old_base / "src" / "gas_calibrator" / "v2" / "output" / "legacy"),
                "logs_dir": str(old_logs_dir),
            },
            "storage": {
                "backend": "sqlite",
                "database": str(old_database),
            },
            "modeling": {
                "data_source": {"path": str(old_model_source)},
                "export": {"output_dir": str(old_logs_dir / "modeling_offline")},
            },
            "_user_tuning_path": str(old_tuning),
            "workflow": {},
        },
        output_dir=current_base / "src" / "gas_calibrator" / "v2" / "output" / "reports",
        temp_c=0.0,
        skip_co2_ppm=[0],
        skip_connect_check=True,
    )

    assert Path(runtime_cfg["_base_dir"]) == current_base.resolve()
    assert Path(runtime_cfg["paths"]["points_excel"]) == (
        current_base / "src" / "gas_calibrator" / "v2" / "configs" / "points.json"
    ).resolve()
    assert Path(runtime_cfg["paths"]["output_dir"]) == (
        current_base / "src" / "gas_calibrator" / "v2" / "output" / "reports"
    ).resolve()
    assert Path(runtime_cfg["paths"]["logs_dir"]) == (
        current_base / "src" / "gas_calibrator" / "v2" / "logs"
    ).resolve()
    assert Path(runtime_cfg["storage"]["database"]) == (
        current_base / "src" / "gas_calibrator" / "v2" / "storage" / "compare.sqlite3"
    ).resolve()
    assert Path(runtime_cfg["modeling"]["data_source"]["path"]) == (
        current_base / "src" / "gas_calibrator" / "v2" / "output" / "v1_v2_compare" / "offline.csv"
    ).resolve()
    assert Path(runtime_cfg["modeling"]["export"]["output_dir"]) == (
        current_base / "src" / "gas_calibrator" / "v2" / "logs" / "modeling_offline"
    ).resolve()
    assert Path(runtime_cfg["_user_tuning_path"]) == (current_base / "configs" / "user_tuning.json").resolve()
    assert runtime_cfg["workflow"]["selected_temps_c"] == [0.0]
    assert runtime_cfg["workflow"]["skip_co2_ppm"] == [0]
    assert runtime_cfg["workflow"]["startup_connect_check"]["enabled"] is False
    assert runtime_cfg["workflow"].get("precheck", {}).get("device_connection", True) is True


def test_resolve_v2_compare_config_path_uses_dedicated_validation_configs_for_default_smoke(
    monkeypatch,
    tmp_path: Path,
) -> None:
    default_v2 = tmp_path / "smoke_v2_minimal.json"
    default_v2.write_text("{}", encoding="utf-8")
    skip0_v2 = tmp_path / "replacement_skip0_real.json"
    skip0_v2.write_text("{}", encoding="utf-8")
    skip0_co2_only_v2 = tmp_path / "replacement_skip0_co2_only_real.json"
    skip0_co2_only_v2.write_text("{}", encoding="utf-8")
    h2o_v2 = tmp_path / "replacement_h2o_only_diagnostic.json"
    h2o_v2.write_text("{}", encoding="utf-8")
    other_v2 = tmp_path / "custom_v2.json"
    other_v2.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(compare_v1_v2_control_flow, "DEFAULT_V2_CONFIG", default_v2)
    monkeypatch.setattr(compare_v1_v2_control_flow, "DEFAULT_SKIP0_CO2_ONLY_V2_CONFIG", skip0_co2_only_v2)
    monkeypatch.setattr(compare_v1_v2_control_flow, "DEFAULT_SKIP0_V2_CONFIG", skip0_v2)
    monkeypatch.setattr(compare_v1_v2_control_flow, "DEFAULT_H2O_ONLY_V2_CONFIG", h2o_v2)

    resolved_skip0_co2_only = compare_v1_v2_control_flow._resolve_v2_compare_config_path(
        requested_path=default_v2,
        validation_profile="skip0_co2_only_replacement",
    )
    resolved_skip0 = compare_v1_v2_control_flow._resolve_v2_compare_config_path(
        requested_path=default_v2,
        validation_profile="skip0_replacement",
    )
    resolved_default = compare_v1_v2_control_flow._resolve_v2_compare_config_path(
        requested_path=default_v2,
        validation_profile="h2o_only_replacement",
    )
    resolved_custom = compare_v1_v2_control_flow._resolve_v2_compare_config_path(
        requested_path=other_v2,
        validation_profile="h2o_only_replacement",
    )
    resolved_standard = compare_v1_v2_control_flow._resolve_v2_compare_config_path(
        requested_path=default_v2,
        validation_profile="standard",
    )

    assert resolved_skip0_co2_only == skip0_co2_only_v2.resolve()
    assert resolved_skip0 == skip0_v2.resolve()
    assert resolved_default == h2o_v2.resolve()
    assert resolved_custom == other_v2
    assert resolved_standard == default_v2


def test_compare_v1_v2_control_flow_main_runs_both_sides_and_writes_report(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    points_path = tmp_path / "points.json"
    _write_points_json(
        points_path,
        [
            {"route": "h2o", "temperature_c": 0.0, "humidity_pct": 50.0, "pressure_hpa": 1000.0},
            {"route": "h2o", "temperature_c": 20.0, "humidity_pct": 50.0, "pressure_hpa": 1000.0},
        ],
    )
    v1_cfg = tmp_path / "v1.json"
    v2_cfg = tmp_path / "v2.json"
    v1_cfg.write_text("{}", encoding="utf-8")
    v2_cfg.write_text("{}", encoding="utf-8")

    base_v1_cfg = {"paths": {"output_dir": str(tmp_path / "unused_v1"), "points_excel": str(points_path)}, "workflow": {}}
    base_v2_cfg = {"paths": {"output_dir": str(tmp_path / "unused_v2"), "points_excel": str(points_path)}, "workflow": {}}
    monkeypatch.setattr(
        compare_v1_v2_control_flow,
        "load_config",
        lambda path: base_v1_cfg,
    )
    monkeypatch.setattr(
        compare_v1_v2_control_flow,
        "load_config_bundle",
        lambda path, simulation_mode=False: (
            str(path),
            base_v2_cfg,
            object(),
        ),
    )

    seen_runtime_cfgs: dict[str, dict] = {}

    def fake_v1_main(argv):
        config_path = Path(argv[argv.index("--config") + 1])
        run_id = argv[argv.index("--run-id") + 1]
        runtime_cfg = json.loads(config_path.read_text(encoding="utf-8"))
        seen_runtime_cfgs["v1"] = runtime_cfg
        run_dir = Path(runtime_cfg["paths"]["output_dir"]) / run_id
        _write_trace(
            run_dir / "route_trace.jsonl",
            [
                '{"route":"co2","action":"route_baseline","point_tag":"co2_groupa_400ppm_1000hpa","result":"ok"}',
                '{"route":"co2","action":"sample_end","point_tag":"co2_groupa_400ppm_1000hpa","result":"ok","actual":{"sample_count":3}}',
            ],
        )
        return 0

    monkeypatch.setattr(compare_v1_v2_control_flow, "_run_v1_trace_inprocess", fake_v1_main)

    class FakeService:
        def __init__(self, config_path: str) -> None:
            runtime_cfg = json.loads(Path(config_path).read_text(encoding="utf-8"))
            seen_runtime_cfgs["v2"] = runtime_cfg
            self.result_store = SimpleNamespace(run_dir=Path(runtime_cfg["paths"]["output_dir"]) / "run_fake_v2")
            self.session = SimpleNamespace(run_id="run_fake_v2")
            self.device_manager = SimpleNamespace(close_all=lambda: None)
            self.is_running = False
            self._status = SimpleNamespace(phase=SimpleNamespace(value="completed"), error=None)

        def run(self) -> None:
            _write_trace(
                self.result_store.run_dir / "route_trace.jsonl",
                [
                    '{"route":"co2","action":"route_baseline","point_tag":"co2_groupa_400ppm_1000hpa","result":"ok"}',
                    '{"route":"co2","action":"sample_end","point_tag":"co2_groupa_400ppm_1000hpa","result":"ok","actual":{"sample_count":3}}',
                ],
            )

        def stop(self, wait: bool = True) -> None:
            return None

        def get_status(self):
            return self._status

        def get_results(self):
            return [1, 2, 3]

    monkeypatch.setattr(
        compare_v1_v2_control_flow,
        "create_calibration_service",
        lambda config_path, simulation_mode=False: FakeService(config_path),
    )

    exit_code = compare_v1_v2_control_flow.main(
        [
            "--v1-config",
            str(v1_cfg),
            "--v2-config",
            str(v2_cfg),
            "--temp",
            "0",
            "--skip-co2-ppm",
            "0",
            "--skip-connect-check",
            "--report-root",
            str(tmp_path / "reports"),
            "--run-name",
            "compare_fixed",
        ]
    )
    captured = capsys.readouterr().out
    report_dir = tmp_path / "reports" / "compare_fixed"
    payload = json.loads((report_dir / "control_flow_compare_report.json").read_text(encoding="utf-8"))

    assert exit_code == 0
    assert seen_runtime_cfgs["v1"]["workflow"]["skip_co2_ppm"] == [0]
    assert seen_runtime_cfgs["v2"]["workflow"]["skip_co2_ppm"] == [0]
    assert seen_runtime_cfgs["v1"]["workflow"]["selected_temps_c"] == [0.0]
    assert seen_runtime_cfgs["v2"]["workflow"]["selected_temps_c"] == [0.0]
    assert "_base_dir" in seen_runtime_cfgs["v1"]
    assert "_base_dir" in seen_runtime_cfgs["v2"]
    assert payload["metadata"]["skip_co2_ppm"] == [0]
    assert "secondary review aid" in payload["validation_scope"]["summary"]
    assert payload["presence"]["matches"] is True
    assert payload["sample_count"]["matches"] is True
    assert payload["route_sequence"]["matches"] is True
    assert payload["compare_status"] == compare_v1_v2_control_flow.COMPARE_STATUS_MATCH
    assert (report_dir / "v1_route_trace.jsonl").exists()
    assert (report_dir / "v2_route_trace.jsonl").exists()
    assert (report_dir / "route_trace_diff.txt").exists()
    assert (report_dir / "point_presence_diff.json").exists()
    assert (report_dir / "sample_count_diff.json").exists()
    assert payload["artifacts"]["v1_route_trace"].endswith("v1_route_trace.jsonl")
    assert payload["artifacts"]["v2_route_trace"].endswith("v2_route_trace.jsonl")
    assert payload["artifacts"]["route_trace_diff"].endswith("route_trace_diff.txt")
    assert payload["artifacts"]["point_presence_diff"].endswith("point_presence_diff.json")
    assert payload["artifacts"]["sample_count_diff"].endswith("sample_count_diff.json")
    assert payload["artifact_inventory"]["complete"] is True
    assert payload["artifact_inventory"]["required"]["route_trace_diff"]["exists"] is True
    assert payload["artifact_inventory"]["required"]["control_flow_compare_report_json"]["exists"] is True
    assert payload["artifact_inventory"]["required"]["control_flow_compare_report_markdown"]["exists"] is True
    assert "Overall status: MATCH" in captured
    assert "V1 route trace:" in captured
    assert "V2 route trace:" in captured
    assert "Route trace diff:" in captured
    assert "Point presence diff:" in captured
    assert "Sample count diff:" in captured
    assert (report_dir / "control_flow_compare_report.md").exists()


def test_compare_v1_v2_control_flow_main_replacement_skip0_preset_sets_skip_list(
    monkeypatch,
    tmp_path: Path,
) -> None:
    points_path = tmp_path / "points.json"
    _write_points_json(
        points_path,
        [
            {"route": "h2o", "temperature_c": 0.0, "humidity_pct": 50.0, "pressure_hpa": 1000.0},
            {"route": "co2", "temperature_c": 0.0, "co2_ppm": 400.0, "pressure_hpa": 1000.0},
        ],
    )
    v1_cfg = tmp_path / "v1.json"
    v2_cfg = tmp_path / "v2.json"
    v1_cfg.write_text("{}", encoding="utf-8")
    v2_cfg.write_text("{}", encoding="utf-8")

    base_v1_cfg = {"paths": {"output_dir": str(tmp_path / "unused_v1"), "points_excel": str(points_path)}, "workflow": {}}
    base_v2_cfg = {"paths": {"output_dir": str(tmp_path / "unused_v2"), "points_excel": str(points_path)}, "workflow": {}}
    monkeypatch.setattr(
        compare_v1_v2_control_flow,
        "load_config",
        lambda path: base_v1_cfg,
    )
    monkeypatch.setattr(
        compare_v1_v2_control_flow,
        "load_config_bundle",
        lambda path, simulation_mode=False: (
            str(path),
            base_v2_cfg,
            object(),
        ),
    )

    seen_runtime_cfgs: dict[str, dict] = {}

    def fake_v1_main(argv):
        config_path = Path(argv[argv.index("--config") + 1])
        run_id = argv[argv.index("--run-id") + 1]
        runtime_cfg = json.loads(config_path.read_text(encoding="utf-8"))
        seen_runtime_cfgs["v1"] = runtime_cfg
        run_dir = Path(runtime_cfg["paths"]["output_dir"]) / run_id
        _write_trace(
            run_dir / "route_trace.jsonl",
            [
                '{"route":"h2o","action":"route_baseline","point_tag":"h2o_20c_50rh_1000hpa","result":"ok"}',
                '{"route":"h2o","action":"sample_end","point_tag":"h2o_20c_50rh_1000hpa","result":"ok","actual":{"sample_count":2}}',
            ],
        )
        return 0

    monkeypatch.setattr(compare_v1_v2_control_flow, "_run_v1_trace_inprocess", fake_v1_main)

    class FakeService:
        def __init__(self, config_path: str) -> None:
            runtime_cfg = json.loads(Path(config_path).read_text(encoding="utf-8"))
            seen_runtime_cfgs["v2"] = runtime_cfg
            self.result_store = SimpleNamespace(run_dir=Path(runtime_cfg["paths"]["output_dir"]) / "run_fake_v2")
            self.session = SimpleNamespace(run_id="run_fake_v2")
            self.device_manager = SimpleNamespace(close_all=lambda: None)
            self.is_running = False
            self._status = SimpleNamespace(phase=SimpleNamespace(value="completed"), error=None)

        def run(self) -> None:
            _write_trace(
                self.result_store.run_dir / "route_trace.jsonl",
                [
                    '{"route":"h2o","action":"route_baseline","point_tag":"h2o_20c_50rh_1000hpa","result":"ok"}',
                    '{"route":"h2o","action":"sample_end","point_tag":"h2o_20c_50rh_1000hpa","result":"ok","actual":{"sample_count":2}}',
                ],
            )

        def stop(self, wait: bool = True) -> None:
            return None

        def get_status(self):
            return self._status

        def get_results(self):
            return [1, 2]

    monkeypatch.setattr(
        compare_v1_v2_control_flow,
        "create_calibration_service",
        lambda config_path, simulation_mode=False: FakeService(config_path),
    )

    exit_code = compare_v1_v2_control_flow.main(
        [
            "--v1-config",
            str(v1_cfg),
            "--v2-config",
            str(v2_cfg),
            "--temp",
            "0",
            "--replacement-skip0",
            "--skip-connect-check",
            "--report-root",
            str(tmp_path / "reports"),
            "--run-name",
            "skip0_fixed",
        ]
    )
    report_dir = tmp_path / "reports" / "skip0_fixed"
    payload = json.loads((report_dir / "control_flow_compare_report.json").read_text(encoding="utf-8"))

    assert exit_code == 0
    assert seen_runtime_cfgs["v1"]["workflow"]["skip_co2_ppm"] == [0]
    assert seen_runtime_cfgs["v2"]["workflow"]["skip_co2_ppm"] == [0]
    assert "_base_dir" in seen_runtime_cfgs["v1"]
    assert "_base_dir" in seen_runtime_cfgs["v2"]
    assert payload["metadata"]["validation_profile"] == "skip0_replacement"
    assert payload["metadata"]["skip_co2_ppm"] == [0]
    assert "Full numeric equivalence between V1 and V2." in payload["validation_scope"]["does_not_prove"]
    assert payload["replacement_validation"]["sample_count_matches"] is True
    bundle_path = report_dir / "skip0_replacement_bundle.json"
    latest_path = tmp_path / "reports" / "skip0_replacement_latest.json"
    assert bundle_path.exists()
    assert latest_path.exists()
    bundle_payload = json.loads(bundle_path.read_text(encoding="utf-8"))
    latest_payload = json.loads(latest_path.read_text(encoding="utf-8"))
    assert bundle_payload["classification"] == "control_flow_replacement_validation"
    assert bundle_payload["checklist_gate"] == "12A"
    assert bundle_payload["validation_profile"] == "skip0_replacement"
    assert bundle_payload["compare_status"] == compare_v1_v2_control_flow.COMPARE_STATUS_MATCH
    assert bundle_payload["bench_context"]["co2_0ppm_available"] is False
    assert bundle_payload["bench_context"]["other_gases_available"] is True
    assert bundle_payload["effective_validation_mode"]["validation_profile"] == "skip0_replacement"
    assert bundle_payload["effective_validation_mode"]["sensor_precheck_validation_mode"] == "snapshot"
    assert bundle_payload["artifacts"]["v1_route_trace"].endswith("v1_route_trace.jsonl")
    assert bundle_payload["artifacts"]["v2_route_trace"].endswith("v2_route_trace.jsonl")
    assert bundle_payload["artifacts"]["route_trace_diff"].endswith("route_trace_diff.txt")
    assert bundle_payload["artifacts"]["point_presence_diff"].endswith("point_presence_diff.json")
    assert bundle_payload["artifacts"]["sample_count_diff"].endswith("sample_count_diff.json")
    assert bundle_payload["artifact_inventory"]["complete"] is True
    assert bundle_payload["artifact_inventory"]["required"]["sample_count_diff"]["exists"] is True
    assert latest_payload["report_dir"] == str(report_dir)
    assert latest_payload["artifact_inventory"]["complete"] is True
    assert latest_payload["compare_status"] == compare_v1_v2_control_flow.COMPARE_STATUS_MATCH
    assert "route_execution_summary" in latest_payload
    assert latest_payload["bench_context"]["co2_0ppm_available"] is False
    assert latest_payload["effective_validation_mode"]["validation_profile"] == "skip0_replacement"


def test_compare_v1_v2_control_flow_main_skip0_co2_only_preset_sets_co2_only_route_and_indexes(
    monkeypatch,
    tmp_path: Path,
) -> None:
    points_path = tmp_path / "points.json"
    _write_points_json(
        points_path,
        [
            {"route": "h2o", "temperature_c": 20.0, "humidity_pct": 50.0, "pressure_hpa": 1000.0},
            {"route": "co2", "temperature_c": 20.0, "co2_ppm": 0.0, "pressure_hpa": 1000.0},
            {"route": "co2", "temperature_c": 20.0, "co2_ppm": 400.0, "pressure_hpa": 1000.0},
        ],
    )
    v1_cfg = tmp_path / "v1.json"
    v2_cfg = tmp_path / "v2.json"
    v1_cfg.write_text("{}", encoding="utf-8")
    v2_cfg.write_text("{}", encoding="utf-8")

    base_v1_cfg = {"paths": {"output_dir": str(tmp_path / "unused_v1"), "points_excel": str(points_path)}, "workflow": {}}
    base_v2_cfg = {"paths": {"output_dir": str(tmp_path / "unused_v2"), "points_excel": str(points_path)}, "workflow": {}}
    monkeypatch.setattr(compare_v1_v2_control_flow, "load_config", lambda path: base_v1_cfg)
    monkeypatch.setattr(
        compare_v1_v2_control_flow,
        "load_config_bundle",
        lambda path, simulation_mode=False: (str(path), base_v2_cfg, object()),
    )

    seen_runtime_cfgs: dict[str, dict] = {}

    def fake_v1_main(argv):
        config_path = Path(argv[argv.index("--config") + 1])
        run_id = argv[argv.index("--run-id") + 1]
        runtime_cfg = json.loads(config_path.read_text(encoding="utf-8"))
        seen_runtime_cfgs["v1"] = runtime_cfg
        run_dir = Path(runtime_cfg["paths"]["output_dir"]) / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "route_trace_status.json").write_text(
            json.dumps({"ok": True, "status_phase": "completed"}, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        _write_trace(
            run_dir / "route_trace.jsonl",
            [
                '{"route":"co2","action":"route_baseline","point_tag":"co2_groupa_400ppm_1000hpa","result":"ok"}',
                '{"route":"co2","action":"sample_end","point_tag":"co2_groupa_400ppm_1000hpa","result":"ok","actual":{"sample_count":2}}',
            ],
        )
        return 0

    monkeypatch.setattr(compare_v1_v2_control_flow, "_run_v1_trace_inprocess", fake_v1_main)

    class FakeService:
        def __init__(self, config_path: str) -> None:
            runtime_cfg = json.loads(Path(config_path).read_text(encoding="utf-8"))
            seen_runtime_cfgs["v2"] = runtime_cfg
            self.result_store = SimpleNamespace(run_dir=Path(runtime_cfg["paths"]["output_dir"]) / "run_fake_v2")
            self.session = SimpleNamespace(run_id="run_fake_v2")
            self.device_manager = SimpleNamespace(close_all=lambda: None)
            self.is_running = False
            self._status = SimpleNamespace(phase=SimpleNamespace(value="completed"), error=None)

        def run(self) -> None:
            _write_trace(
                self.result_store.run_dir / "route_trace.jsonl",
                [
                    '{"route":"co2","action":"route_baseline","point_tag":"co2_groupa_400ppm_1000hpa","result":"ok"}',
                    '{"route":"co2","action":"sample_end","point_tag":"co2_groupa_400ppm_1000hpa","result":"ok","actual":{"sample_count":2}}',
                ],
            )

        def stop(self, wait: bool = True) -> None:
            return None

        def get_status(self):
            return self._status

        def get_results(self):
            return [1, 2]

    monkeypatch.setattr(
        compare_v1_v2_control_flow,
        "create_calibration_service",
        lambda config_path, simulation_mode=False: FakeService(config_path),
    )

    exit_code = compare_v1_v2_control_flow.main(
        [
            "--v1-config",
            str(v1_cfg),
            "--v2-config",
            str(v2_cfg),
            "--temp",
            "20",
            "--replacement-skip0-co2-only",
            "--skip-connect-check",
            "--report-root",
            str(tmp_path / "reports"),
            "--run-name",
            "skip0_co2_only_fixed",
        ]
    )
    report_dir = tmp_path / "reports" / "skip0_co2_only_fixed"
    payload = json.loads((report_dir / "control_flow_compare_report.json").read_text(encoding="utf-8"))
    bundle_payload = json.loads((report_dir / "skip0_co2_only_replacement_bundle.json").read_text(encoding="utf-8"))
    latest_payload = json.loads((tmp_path / "reports" / "skip0_co2_only_replacement_latest.json").read_text(encoding="utf-8"))

    assert exit_code == 0
    assert seen_runtime_cfgs["v1"]["workflow"]["route_mode"] == "co2_only"
    assert seen_runtime_cfgs["v2"]["workflow"]["route_mode"] == "co2_only"
    assert seen_runtime_cfgs["v1"]["workflow"]["skip_co2_ppm"] == [0]
    assert seen_runtime_cfgs["v2"]["workflow"]["skip_co2_ppm"] == [0]
    assert seen_runtime_cfgs["v1"]["devices"]["humidity_generator"]["enabled"] is False
    assert seen_runtime_cfgs["v2"]["devices"]["humidity_generator"]["enabled"] is False
    assert seen_runtime_cfgs["v1"]["devices"]["dewpoint_meter"]["enabled"] is False
    assert seen_runtime_cfgs["v2"]["devices"]["dewpoint_meter"]["enabled"] is False
    assert seen_runtime_cfgs["v2"]["workflow"]["stability"]["humidity_generator"]["enabled"] is False
    assert payload["metadata"]["validation_profile"] == "skip0_co2_only_replacement"
    assert payload["metadata"]["route_mode"] == "co2_only"
    assert payload["bench_context"]["h2o_route_available"] is False
    assert payload["bench_context"]["humidity_generator_humidity_feedback_valid"] is False
    assert payload["route_execution_summary"]["target_route"] == "co2"
    assert payload["compare_status"] == compare_v1_v2_control_flow.COMPARE_STATUS_MATCH
    assert payload["effective_validation_mode"]["target_route"] == "co2"
    assert payload["effective_validation_mode"]["sensor_precheck_validation_mode"] == "v1_frame_like"
    assert payload["effective_validation_mode"]["sensor_precheck_active_send"] is True
    assert payload["effective_validation_mode"]["sensor_precheck_strict"] is True
    assert bundle_payload["checklist_gate"] == "12A"
    assert bundle_payload["validation_profile"] == "skip0_co2_only_replacement"
    assert bundle_payload["evidence_state"] == "current_primary_validation"
    assert bundle_payload["bench_context"]["h2o_route_available"] is False
    assert latest_payload["validation_profile"] == "skip0_co2_only_replacement"
    assert latest_payload["compare_status"] == compare_v1_v2_control_flow.COMPARE_STATUS_MATCH


def test_compare_v1_v2_control_flow_main_h2o_only_preset_sets_route_mode_and_indexes(
    monkeypatch,
    tmp_path: Path,
) -> None:
    points_path = tmp_path / "points.json"
    _write_points_json(
        points_path,
        [
            {"route": "h2o", "temperature_c": 20.0, "humidity_pct": 50.0, "pressure_hpa": 1000.0},
            {"route": "h2o", "temperature_c": 0.0, "humidity_pct": 50.0, "pressure_hpa": 1000.0},
        ],
    )
    v1_cfg = tmp_path / "v1.json"
    v2_cfg = tmp_path / "v2.json"
    v1_cfg.write_text("{}", encoding="utf-8")
    v2_cfg.write_text("{}", encoding="utf-8")

    base_v1_cfg = {"paths": {"output_dir": str(tmp_path / "unused_v1"), "points_excel": str(points_path)}, "workflow": {}}
    base_v2_cfg = {"paths": {"output_dir": str(tmp_path / "unused_v2"), "points_excel": str(points_path)}, "workflow": {}}
    monkeypatch.setattr(
        compare_v1_v2_control_flow,
        "load_config",
        lambda path: base_v1_cfg,
    )
    monkeypatch.setattr(
        compare_v1_v2_control_flow,
        "load_config_bundle",
        lambda path, simulation_mode=False: (
            str(path),
            base_v2_cfg,
            object(),
        ),
    )

    seen_runtime_cfgs: dict[str, dict] = {}
    seen_v1_argv: list[str] = []

    def fake_v1_main(argv):
        seen_v1_argv[:] = list(argv)
        config_path = Path(argv[argv.index("--config") + 1])
        run_id = argv[argv.index("--run-id") + 1]
        runtime_cfg = json.loads(config_path.read_text(encoding="utf-8"))
        seen_runtime_cfgs["v1"] = runtime_cfg
        run_dir = Path(runtime_cfg["paths"]["output_dir"]) / run_id
        _write_trace(
            run_dir / "route_trace.jsonl",
            [
                '{"route":"h2o","action":"route_baseline","point_tag":"h2o_20c_50rh_1000hpa","result":"ok"}',
                '{"route":"h2o","action":"sample_end","point_tag":"h2o_20c_50rh_1000hpa","result":"ok","actual":{"sample_count":2}}',
            ],
        )
        return 0

    monkeypatch.setattr(compare_v1_v2_control_flow, "_run_v1_trace_inprocess", fake_v1_main)

    class FakeService:
        def __init__(self, config_path: str) -> None:
            runtime_cfg = json.loads(Path(config_path).read_text(encoding="utf-8"))
            seen_runtime_cfgs["v2"] = runtime_cfg
            self.result_store = SimpleNamespace(run_dir=Path(runtime_cfg["paths"]["output_dir"]) / "run_fake_v2")
            self.session = SimpleNamespace(run_id="run_fake_v2")
            self.device_manager = SimpleNamespace(close_all=lambda: None)
            self.is_running = False
            self._status = SimpleNamespace(phase=SimpleNamespace(value="completed"), error=None)

        def run(self) -> None:
            _write_trace(
                self.result_store.run_dir / "route_trace.jsonl",
                [
                    '{"route":"h2o","action":"route_baseline","point_tag":"h2o_20c_50rh_1000hpa","result":"ok"}',
                    '{"route":"h2o","action":"sample_end","point_tag":"h2o_20c_50rh_1000hpa","result":"ok","actual":{"sample_count":2}}',
                ],
            )

        def stop(self, wait: bool = True) -> None:
            return None

        def get_status(self):
            return self._status

        def get_results(self):
            return [1, 2]

    monkeypatch.setattr(
        compare_v1_v2_control_flow,
        "create_calibration_service",
        lambda config_path, simulation_mode=False: FakeService(config_path),
    )

    exit_code = compare_v1_v2_control_flow.main(
        [
            "--v1-config",
            str(v1_cfg),
            "--v2-config",
            str(v2_cfg),
            "--temp",
            "20",
            "--replacement-h2o-only",
            "--skip-connect-check",
            "--report-root",
            str(tmp_path / "reports"),
            "--run-name",
            "h2o_only_fixed",
        ]
    )
    report_dir = tmp_path / "reports" / "h2o_only_fixed"
    payload = json.loads((report_dir / "control_flow_compare_report.json").read_text(encoding="utf-8"))

    assert exit_code == 0
    assert "--h2o-only" in seen_v1_argv
    assert seen_runtime_cfgs["v1"]["workflow"]["route_mode"] == "h2o_only"
    assert seen_runtime_cfgs["v2"]["workflow"]["route_mode"] == "h2o_only"
    assert seen_runtime_cfgs["v1"]["workflow"]["collect_only"] is True
    assert seen_runtime_cfgs["v2"]["workflow"]["collect_only"] is True
    assert seen_runtime_cfgs["v2"]["workflow"]["collect_only_fast_path"] is True
    assert seen_runtime_cfgs["v1"]["workflow"]["precheck"]["device_connection"] is False
    assert seen_runtime_cfgs["v2"]["workflow"]["precheck"]["device_connection"] is False
    assert seen_runtime_cfgs["v1"]["workflow"]["precheck"]["sensor_check"] is False
    assert seen_runtime_cfgs["v2"]["workflow"]["precheck"]["sensor_check"] is False
    assert seen_runtime_cfgs["v2"]["workflow"]["sensor_precheck"]["enabled"] is True
    assert seen_runtime_cfgs["v2"]["workflow"]["sensor_precheck"]["profile"] == "raw_frame_first"
    assert seen_runtime_cfgs["v2"]["workflow"]["sensor_precheck"]["scope"] == "first_analyzer_only"
    assert seen_runtime_cfgs["v2"]["workflow"]["sensor_precheck"]["validation_mode"] == "v1_frame_like"
    assert seen_runtime_cfgs["v2"]["workflow"]["sensor_precheck"]["strict"] is False
    assert seen_runtime_cfgs["v1"]["devices"]["pressure_controller"]["in_limits_time_s"] == 0.2
    assert seen_runtime_cfgs["v1"]["workflow"]["sampling"]["count"] == 1
    assert seen_runtime_cfgs["v1"]["workflow"]["sampling"]["stable_count"] == 1
    assert seen_runtime_cfgs["v1"]["workflow"]["stability"]["temperature"]["analyzer_chamber_temp_enabled"] is False
    assert seen_runtime_cfgs["v1"]["workflow"]["stability"]["h2o_route"]["preseal_soak_s"] == 0.1
    assert seen_runtime_cfgs["v1"]["workflow"]["stability"]["h2o_route"]["humidity_timeout_policy"] == "abort_like_v1"
    assert seen_runtime_cfgs["v1"]["workflow"]["stability"]["humidity_generator"]["timeout_s"] == 2.0
    assert seen_runtime_cfgs["v1"]["workflow"]["stability"]["dewpoint"]["timeout_s"] == 2.0
    assert payload["metadata"]["validation_profile"] == "h2o_only_replacement"
    assert payload["metadata"]["route_mode"] == "h2o_only"
    assert "fallback diagnostic route" in payload["validation_scope"]["summary"]
    assert payload["compare_status"] == compare_v1_v2_control_flow.COMPARE_STATUS_MATCH
    assert payload["entered_target_route"] == {"v1": True, "v2": True}
    assert payload["valid_for_route_diff"] is True
    assert payload["validation_scope"]["does_not_prove"][0] == "CO2 route or gas-path behavior equivalence."
    bundle_path = report_dir / "h2o_only_replacement_bundle.json"
    latest_path = tmp_path / "reports" / "h2o_only_replacement_latest.json"
    assert bundle_path.exists()
    assert latest_path.exists()
    bundle_payload = json.loads(bundle_path.read_text(encoding="utf-8"))
    latest_payload = json.loads(latest_path.read_text(encoding="utf-8"))
    assert bundle_payload["validation_profile"] == "h2o_only_replacement"
    assert bundle_payload["checklist_gate"] == "12B"
    assert bundle_payload["bench_context"]["validation_role"] == "diagnostic"
    assert bundle_payload["artifacts"]["route_trace_diff"].endswith("route_trace_diff.txt")
    assert bundle_payload["artifacts"]["point_presence_diff"].endswith("point_presence_diff.json")
    assert bundle_payload["artifacts"]["sample_count_diff"].endswith("sample_count_diff.json")
    assert bundle_payload["artifact_inventory"]["complete"] is True
    assert latest_payload["report_dir"] == str(report_dir)
    assert latest_payload["compare_status"] == compare_v1_v2_control_flow.COMPARE_STATUS_MATCH
    assert latest_payload["effective_validation_mode"]["validation_profile"] == "h2o_only_replacement"
    assert payload["route_execution_summary"]["sides"]["v2"]["runtime_policy"]["precheck_sensor_check"] is False
    assert payload["route_execution_summary"]["sides"]["v2"]["runtime_policy"]["sensor_precheck_enabled"] is True
    assert payload["route_execution_summary"]["sides"]["v2"]["runtime_policy"]["sensor_precheck_profile"] == "raw_frame_first"
    assert payload["route_execution_summary"]["sides"]["v2"]["runtime_policy"]["sensor_precheck_scope"] == "first_analyzer_only"
    assert payload["route_execution_summary"]["sides"]["v2"]["runtime_policy"]["sensor_precheck_validation_mode"] == "v1_frame_like"
    assert payload["route_execution_summary"]["sides"]["v2"]["runtime_policy"]["sensor_precheck_strict"] is False


def test_build_control_flow_report_marks_target_route_not_executed(tmp_path: Path) -> None:
    v1_trace = tmp_path / "v1.jsonl"
    v2_trace = tmp_path / "v2.jsonl"
    _write_trace(
        v1_trace,
        [
            '{"route":"h2o","action":"set_h2o_path","point_tag":"h2o_20c_50rh_1000hpa","result":"ok"}',
            '{"route":"h2o","action":"wait_humidity","point_tag":"h2o_20c_50rh_1000hpa","result":"timeout"}',
        ],
    )
    _write_trace(
        v2_trace,
        [
            '{"route":"init","action":"analyzer_setup_profile","point_tag":"","result":"ok"}',
            '{"route":"baseline","action":"restore_baseline","point_tag":"","result":"ok"}',
        ],
    )

    report = compare_v1_v2_control_flow.build_control_flow_report(
        v1_trace_path=v1_trace,
        v2_trace_path=v2_trace,
        metadata={
            "validation_profile": "h2o_only_replacement",
            "route_mode": "h2o_only",
            "v1": {"ok": False, "status_phase": "completed"},
            "v2": {
                "ok": False,
                "status_phase": "precheck",
                "status_error": "Device precheck failed [failed_devices=['gas_analyzer_0']]",
            },
        },
    )

    artifacts = compare_v1_v2_control_flow._write_compare_side_artifacts(
        report_dir=tmp_path / "report",
        report=report,
        v1_trace_path=v1_trace,
        v2_trace_path=v2_trace,
    )
    route_text = Path(artifacts["route_trace_diff"]).read_text(encoding="utf-8")

    assert report["compare_status"] == compare_v1_v2_control_flow.COMPARE_STATUS_NOT_EXECUTED
    assert report["entered_target_route"] == {"v1": True, "v2": False}
    assert report["target_route_event_count"] == {"v1": 2, "v2": 0}
    assert report["valid_for_route_diff"] is False
    assert report["first_failure_phase"] == "v2:precheck.device_connection"
    assert report["route_execution_summary"]["reason"] == "target route `h2o` was not entered on: v2"
    assert report["replacement_validation"]["presence_evaluable"] is False
    assert report["replacement_validation"]["sample_count_evaluable"] is False
    assert report["replacement_validation"]["route_action_order_evaluable"] is False
    assert report["replacement_validation"]["presence_matches"] is None
    assert report["replacement_validation"]["sample_count_matches"] is None
    assert report["replacement_validation"]["route_action_order_matches"] is None
    assert "Compare status: NOT_EXECUTED" in route_text
    assert "Valid for route diff: False" in route_text


def test_compare_v1_v2_control_flow_main_marks_invalid_profile_input_before_run(
    monkeypatch,
    tmp_path: Path,
) -> None:
    points_path = tmp_path / "points.json"
    _write_points_json(
        points_path,
        [
            {"route": "h2o", "temperature_c": 0.0, "humidity_pct": 50.0, "pressure_hpa": 1000.0},
        ],
    )
    v1_cfg = tmp_path / "v1.json"
    v2_cfg = tmp_path / "v2.json"
    v1_cfg.write_text("{}", encoding="utf-8")
    v2_cfg.write_text("{}", encoding="utf-8")

    base_cfg = {"paths": {"output_dir": str(tmp_path / "unused"), "points_excel": str(points_path)}, "workflow": {}}
    monkeypatch.setattr(compare_v1_v2_control_flow, "load_config", lambda path: base_cfg)
    monkeypatch.setattr(
        compare_v1_v2_control_flow,
        "load_config_bundle",
        lambda path, simulation_mode=False: (str(path), base_cfg, object()),
    )
    monkeypatch.setattr(
        compare_v1_v2_control_flow,
        "_run_v1_trace_inprocess",
        lambda argv: (_ for _ in ()).throw(AssertionError("V1 trace should not run for invalid preset input")),
    )
    monkeypatch.setattr(
        compare_v1_v2_control_flow,
        "create_calibration_service",
        lambda config_path, simulation_mode=False: (_ for _ in ()).throw(
            AssertionError("V2 service should not run for invalid preset input")
        ),
    )

    exit_code = compare_v1_v2_control_flow.main(
        [
            "--v1-config",
            str(v1_cfg),
            "--v2-config",
            str(v2_cfg),
            "--temp",
            "20",
            "--replacement-h2o-only",
            "--report-root",
            str(tmp_path / "reports"),
            "--run-name",
            "invalid_profile",
        ]
    )
    report_path = tmp_path / "reports" / "invalid_profile" / "control_flow_compare_report.json"
    route_diff_path = tmp_path / "reports" / "invalid_profile" / "route_trace_diff.txt"
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    route_text = route_diff_path.read_text(encoding="utf-8")

    assert exit_code == 1
    assert payload["compare_status"] == compare_v1_v2_control_flow.COMPARE_STATUS_INVALID_PROFILE_INPUT
    assert payload["first_failure_phase"] == "v1:input_validation.points_filter"
    assert payload["valid_for_route_diff"] is False
    assert payload["replacement_validation"]["presence_evaluable"] is False
    assert payload["replacement_validation"]["sample_count_evaluable"] is False
    assert payload["replacement_validation"]["route_action_order_evaluable"] is False
    assert payload["replacement_validation"]["presence_matches"] is None
    assert payload["replacement_validation"]["sample_count_matches"] is None
    assert payload["replacement_validation"]["route_action_order_matches"] is None
    assert payload["metadata"]["preflight"]["ok"] is False
    assert payload["metadata"]["preflight"]["sides"]["v1"]["available_temps"] == [0.0]
    assert payload["metadata"]["preflight"]["sides"]["v1"]["requested_temps"] == [20.0]
    assert payload["metadata"]["preflight"]["sides"]["v1"]["filtered_count"] == 0
    assert "filtered point set is empty" in payload["metadata"]["preflight"]["sides"]["v1"]["reason"]
    assert "Compare status: INVALID_PROFILE_INPUT" in route_text
    assert "available_temps=[0.0]" in route_text


def test_build_control_flow_report_marks_mixed_skip0_not_executed_when_compare_scope_never_starts(tmp_path: Path) -> None:
    v1_trace = tmp_path / "v1_empty.jsonl"
    v2_trace = tmp_path / "v2_empty.jsonl"
    _write_trace(v1_trace, [])
    _write_trace(v2_trace, [])

    report = compare_v1_v2_control_flow.build_control_flow_report(
        v1_trace_path=v1_trace,
        v2_trace_path=v2_trace,
        metadata={
            "validation_profile": "skip0_replacement",
            "skip_co2_ppm": [0],
            "v1": {"ok": False, "status_phase": "startup.device_connection.port_busy"},
            "v2": {"ok": False, "status_phase": "startup.sensor_precheck"},
        },
    )

    assert report["compare_status"] == compare_v1_v2_control_flow.COMPARE_STATUS_NOT_EXECUTED
    assert report["route_execution_summary"]["target_route"] is None
    assert report["route_execution_summary"]["valid_for_route_diff"] is False
    assert report["replacement_validation"]["presence_evaluable"] is False
    assert report["replacement_validation"]["sample_count_evaluable"] is False
    assert report["replacement_validation"]["route_action_order_evaluable"] is False
    assert report["replacement_validation"]["key_action_groups_evaluable"] is False
    assert report["replacement_validation"]["presence_matches"] is None
    assert report["replacement_validation"]["sample_count_matches"] is None
    assert report["replacement_validation"]["route_action_order_matches"] is None


def test_run_v1_trace_reads_structured_status_file(monkeypatch, tmp_path: Path) -> None:
    runtime_cfg_path = tmp_path / "runtime_v1.json"
    runtime_cfg_path.write_text(
        json.dumps({"paths": {"output_dir": str(tmp_path / "v1_output")}}, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    def fake_v1_main(argv):
        run_id = argv[argv.index("--run-id") + 1]
        run_dir = tmp_path / "v1_output" / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "route_trace_status.json").write_text(
            json.dumps(
                {
                    "ok": False,
                    "status_phase": "startup.device_connection.port_busy",
                    "status_error": "Permission denied on COM24",
                    "error_category": "startup.device_connection.port_busy",
                    "derived_failure_phase": "startup.device_connection.port_busy",
                    "last_runner_stage": "startup / sensor precheck",
                    "last_runner_event": "run-aborted",
                    "abort_message": "Permission denied on COM24",
                    "trace_expected_but_missing": True,
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        return 1

    monkeypatch.setattr(compare_v1_v2_control_flow, "_run_v1_trace_inprocess", fake_v1_main)

    payload = compare_v1_v2_control_flow._run_v1_trace(
        runtime_cfg_path,
        temp_c=20.0,
        h2o_only=False,
        skip_connect_check=True,
        run_id="v1_structured_status",
    )

    assert payload["ok"] is False
    assert payload["status_phase"] == "startup.device_connection.port_busy"
    assert payload["status_error"] == "Permission denied on COM24"
    assert payload["error_category"] == "startup.device_connection.port_busy"
    assert payload["derived_failure_phase"] == "startup.device_connection.port_busy"
    assert payload["last_runner_stage"] == "startup / sensor precheck"
    assert payload["last_runner_event"] == "run-aborted"
    assert payload["abort_message"] == "Permission denied on COM24"
    assert payload["trace_expected_but_missing"] is True


def test_run_v1_trace_derives_failure_from_io_log_when_status_file_missing(monkeypatch, tmp_path: Path) -> None:
    runtime_cfg_path = tmp_path / "runtime_v1.json"
    runtime_cfg_path.write_text(
        json.dumps({"paths": {"output_dir": str(tmp_path / "v1_output")}}, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    def fake_v1_main(argv):
        run_id = argv[argv.index("--run-id") + 1]
        run_dir = tmp_path / "v1_output" / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "io_20260325_151947.csv").write_text(
            "\n".join(
                [
                    "timestamp,port,device,command,response,error",
                    '2026-03-25T15:19:47,RUN,runner,stage,"startup / sensor precheck",',
                    "2026-03-25T15:19:48,RUN,runner,run-aborted,,stream verify not full MODE2 last=",
                ]
            )
            + "\n",
            encoding="utf-8-sig",
        )
        return 1

    monkeypatch.setattr(compare_v1_v2_control_flow, "_run_v1_trace_inprocess", fake_v1_main)

    payload = compare_v1_v2_control_flow._run_v1_trace(
        runtime_cfg_path,
        temp_c=20.0,
        h2o_only=False,
        skip_connect_check=True,
        run_id="v1_missing_status",
    )

    assert payload["ok"] is False
    assert payload["status_phase"] == "output.route_trace_missing"
    assert payload["error_category"] == "startup.sensor_precheck.mode2_verify"
    assert payload["derived_failure_phase"] == "startup.sensor_precheck.mode2_verify"
    assert payload["last_runner_stage"] == "startup / sensor precheck"
    assert payload["last_runner_event"] == "run-aborted"
    assert payload["abort_message"] == "stream verify not full MODE2 last="
    assert payload["trace_expected_but_missing"] is True


def test_build_control_flow_report_marks_strict_co2_only_not_executed_and_not_evaluable(tmp_path: Path) -> None:
    v1_trace = tmp_path / "v1_init_only.jsonl"
    v2_trace = tmp_path / "v2_init_only.jsonl"
    _write_trace(v1_trace, [])
    _write_trace(
        v2_trace,
        [
            '{"route":"init","action":"sensor_precheck_profile","point_tag":"","result":"ok"}',
            '{"route":"init","action":"sensor_precheck_analyzer","point_tag":"","result":"fail"}',
        ],
    )

    report = compare_v1_v2_control_flow.build_control_flow_report(
        v1_trace_path=v1_trace,
        v2_trace_path=v2_trace,
        metadata={
            "validation_profile": "skip0_co2_only_replacement",
            "route_mode": "co2_only",
            "skip_co2_ppm": [0],
            "v1": {
                "ok": False,
                "status_phase": "output.route_trace_missing",
                "derived_failure_phase": "startup.sensor_precheck.mode2_verify",
            },
            "v2": {
                "ok": False,
                "status_phase": "startup.sensor_precheck",
                "status_error": "sensor_precheck_analyzer valid_frames=0/3",
            },
        },
    )

    assert report["compare_status"] == compare_v1_v2_control_flow.COMPARE_STATUS_NOT_EXECUTED
    assert report["route_execution_summary"]["target_route"] == "co2"
    assert report["first_failure_phase"] == "v1:startup.sensor_precheck.mode2_verify"
    assert report["replacement_validation"]["presence_evaluable"] is False
    assert report["replacement_validation"]["sample_count_evaluable"] is False
    assert report["replacement_validation"]["route_action_order_evaluable"] is False
    assert report["replacement_validation"]["key_action_groups_evaluable"] is False
    assert report["replacement_validation"]["presence_matches"] is None
    assert report["replacement_validation"]["sample_count_matches"] is None
    assert report["replacement_validation"]["route_action_order_matches"] is None


def test_compare_v1_v2_control_flow_main_writes_strict_latest_even_when_v1_trace_is_missing(
    monkeypatch,
    tmp_path: Path,
) -> None:
    points_path = tmp_path / "points.json"
    _write_points_json(
        points_path,
        [
            {"route": "co2", "temperature_c": 20.0, "co2_ppm": 0.0, "pressure_hpa": 1000.0},
            {"route": "co2", "temperature_c": 20.0, "co2_ppm": 400.0, "pressure_hpa": 1000.0},
        ],
    )
    v1_cfg = tmp_path / "v1.json"
    v2_cfg = tmp_path / "v2.json"
    v1_cfg.write_text("{}", encoding="utf-8")
    v2_cfg.write_text("{}", encoding="utf-8")

    base_cfg = {"paths": {"output_dir": str(tmp_path / "unused"), "points_excel": str(points_path)}, "workflow": {}}
    monkeypatch.setattr(compare_v1_v2_control_flow, "load_config", lambda path: base_cfg)
    monkeypatch.setattr(
        compare_v1_v2_control_flow,
        "load_config_bundle",
        lambda path, simulation_mode=False: (str(path), base_cfg, object()),
    )

    def fake_v1_main(argv):
        config_path = Path(argv[argv.index("--config") + 1])
        run_id = argv[argv.index("--run-id") + 1]
        runtime_cfg = json.loads(config_path.read_text(encoding="utf-8"))
        run_dir = Path(runtime_cfg["paths"]["output_dir"]) / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "route_trace_status.json").write_text(
            json.dumps(
                {
                    "ok": False,
                    "status_phase": "output.route_trace_missing",
                    "status_error": "route trace file was not produced",
                    "error_category": "startup.sensor_precheck.mode2_verify",
                    "derived_failure_phase": "startup.sensor_precheck.mode2_verify",
                    "last_runner_stage": "startup / sensor precheck",
                    "last_runner_event": "run-aborted",
                    "abort_message": "stream verify not full MODE2 last=",
                    "trace_expected_but_missing": True,
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        return 1

    monkeypatch.setattr(compare_v1_v2_control_flow, "_run_v1_trace_inprocess", fake_v1_main)

    class FakeService:
        def __init__(self, config_path: str) -> None:
            runtime_cfg = json.loads(Path(config_path).read_text(encoding="utf-8"))
            self.result_store = SimpleNamespace(run_dir=Path(runtime_cfg["paths"]["output_dir"]) / "run_fake_v2")
            self.session = SimpleNamespace(run_id="run_fake_v2")
            self.device_manager = SimpleNamespace(close_all=lambda: None)
            self.is_running = False
            self._status = SimpleNamespace(phase=SimpleNamespace(value="precheck"), error="sensor_precheck_analyzer")

        def run(self) -> None:
            _write_trace(
                self.result_store.run_dir / "route_trace.jsonl",
                [
                    '{"route":"init","action":"sensor_precheck_profile","point_tag":"","result":"ok"}',
                    '{"route":"init","action":"sensor_precheck_analyzer","point_tag":"","result":"fail"}',
                ],
            )

        def stop(self, wait: bool = True) -> None:
            return None

        def get_status(self):
            return self._status

        def get_results(self):
            return []

    monkeypatch.setattr(
        compare_v1_v2_control_flow,
        "create_calibration_service",
        lambda config_path, simulation_mode=False: FakeService(config_path),
    )

    exit_code = compare_v1_v2_control_flow.main(
        [
            "--v1-config",
            str(v1_cfg),
            "--v2-config",
            str(v2_cfg),
            "--temp",
            "20",
            "--replacement-skip0-co2-only",
            "--report-root",
            str(tmp_path / "reports"),
            "--run-name",
            "strict_missing_trace",
        ]
    )
    report_dir = tmp_path / "reports" / "strict_missing_trace"
    report_payload = json.loads((report_dir / "control_flow_compare_report.json").read_text(encoding="utf-8"))
    bundle_payload = json.loads(
        (report_dir / "skip0_co2_only_replacement_bundle.json").read_text(encoding="utf-8")
    )
    latest_payload = json.loads(
        (tmp_path / "reports" / "skip0_co2_only_replacement_latest.json").read_text(encoding="utf-8")
    )

    assert exit_code == 1
    assert report_payload["compare_status"] == compare_v1_v2_control_flow.COMPARE_STATUS_NOT_EXECUTED
    assert report_payload["first_failure_phase"] == "v1:startup.sensor_precheck.mode2_verify"
    assert report_payload["route_execution_summary"]["target_route"] == "co2"
    assert report_payload["artifact_inventory"]["complete"] is False
    assert report_payload["artifact_inventory"]["required"]["v1_route_trace"]["exists"] is False
    assert report_payload["artifacts"]["control_flow_compare_report_json"].endswith("control_flow_compare_report.json")
    assert report_payload["artifacts"]["route_trace_diff"].endswith("route_trace_diff.txt")
    assert (report_dir / "control_flow_compare_report.md").exists()
    assert (report_dir / "artifact_inventory.json").exists()
    assert bundle_payload["validation_profile"] == "skip0_co2_only_replacement"
    assert bundle_payload["diagnostic_only"] is False
    assert bundle_payload["acceptance_evidence"] is True
    assert latest_payload["validation_profile"] == "skip0_co2_only_replacement"
    assert latest_payload["route_execution_summary"]["sides"]["v1"]["derived_failure_phase"] == (
        "startup.sensor_precheck.mode2_verify"
    )


def test_compare_v1_v2_control_flow_main_skip0_co2_only_diagnostic_relaxed_marks_diagnostic_only(
    monkeypatch,
    tmp_path: Path,
) -> None:
    points_path = tmp_path / "points.json"
    _write_points_json(
        points_path,
        [
            {"route": "co2", "temperature_c": 20.0, "co2_ppm": 0.0, "pressure_hpa": 1000.0},
            {"route": "co2", "temperature_c": 20.0, "co2_ppm": 400.0, "pressure_hpa": 1000.0},
        ],
    )
    v1_cfg = tmp_path / "v1.json"
    v2_cfg = tmp_path / "v2.json"
    v1_cfg.write_text("{}", encoding="utf-8")
    v2_cfg.write_text("{}", encoding="utf-8")

    base_cfg = {"paths": {"output_dir": str(tmp_path / "unused"), "points_excel": str(points_path)}, "workflow": {}}
    monkeypatch.setattr(compare_v1_v2_control_flow, "load_config", lambda path: base_cfg)
    monkeypatch.setattr(
        compare_v1_v2_control_flow,
        "load_config_bundle",
        lambda path, simulation_mode=False: (str(path), base_cfg, object()),
    )

    def fake_v1_main(argv):
        config_path = Path(argv[argv.index("--config") + 1])
        run_id = argv[argv.index("--run-id") + 1]
        runtime_cfg = json.loads(config_path.read_text(encoding="utf-8"))
        run_dir = Path(runtime_cfg["paths"]["output_dir"]) / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "route_trace_status.json").write_text(
            json.dumps({"ok": True, "status_phase": "completed"}, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        _write_trace(
            run_dir / "route_trace.jsonl",
            [
                '{"route":"co2","action":"route_baseline","point_tag":"co2_groupa_400ppm_1000hpa","result":"ok"}',
                '{"route":"co2","action":"sample_end","point_tag":"co2_groupa_400ppm_1000hpa","result":"ok","actual":{"sample_count":2}}',
            ],
        )
        return 0

    monkeypatch.setattr(compare_v1_v2_control_flow, "_run_v1_trace_inprocess", fake_v1_main)

    class FakeService:
        def __init__(self, config_path: str) -> None:
            runtime_cfg = json.loads(Path(config_path).read_text(encoding="utf-8"))
            self._runtime_cfg = runtime_cfg
            self.result_store = SimpleNamespace(run_dir=Path(runtime_cfg["paths"]["output_dir"]) / "run_fake_v2")
            self.session = SimpleNamespace(run_id="run_fake_v2")
            self.device_manager = SimpleNamespace(close_all=lambda: None)
            self.is_running = False
            self._status = SimpleNamespace(phase=SimpleNamespace(value="completed"), error=None)

        def run(self) -> None:
            _write_trace(
                self.result_store.run_dir / "route_trace.jsonl",
                [
                    '{"route":"co2","action":"route_baseline","point_tag":"co2_groupa_400ppm_1000hpa","result":"ok"}',
                    '{"route":"co2","action":"sample_end","point_tag":"co2_groupa_400ppm_1000hpa","result":"ok","actual":{"sample_count":2}}',
                ],
            )

        def stop(self, wait: bool = True) -> None:
            return None

        def get_status(self):
            return self._status

        def get_results(self):
            return [1, 2]

    monkeypatch.setattr(
        compare_v1_v2_control_flow,
        "create_calibration_service",
        lambda config_path, simulation_mode=False: FakeService(config_path),
    )

    exit_code = compare_v1_v2_control_flow.main(
        [
            "--v1-config",
            str(v1_cfg),
            "--v2-config",
            str(v2_cfg),
            "--temp",
            "20",
            "--replacement-skip0-co2-only-diagnostic-relaxed",
            "--report-root",
            str(tmp_path / "reports"),
            "--run-name",
            "diag_relaxed",
        ]
    )
    report_dir = tmp_path / "reports" / "diag_relaxed"
    report_payload = json.loads((report_dir / "control_flow_compare_report.json").read_text(encoding="utf-8"))
    latest_payload = json.loads(
        (tmp_path / "reports" / "skip0_co2_only_diagnostic_relaxed_latest.json").read_text(encoding="utf-8")
    )

    assert exit_code == 0
    assert report_payload["metadata"]["validation_profile"] == "skip0_co2_only_diagnostic_relaxed"
    assert report_payload["metadata"]["route_mode"] == "co2_only"
    assert report_payload["effective_validation_mode"]["target_route"] == "co2"
    assert report_payload["effective_validation_mode"]["sensor_precheck_active_send"] is False
    assert report_payload["effective_validation_mode"]["sensor_precheck_strict"] is False
    assert report_payload["bench_context"]["diagnostic_only"] is True
    assert report_payload["bench_context"]["acceptance_evidence"] is False
    assert report_payload["bench_context"]["h2o_route_available"] is False
    assert report_payload["route_execution_summary"]["target_route"] == "co2"
    assert report_payload["route_execution_summary"]["sides"]["v2"]["runtime_policy"]["sensor_precheck_active_send"] is False
    assert latest_payload["diagnostic_only"] is True
    assert latest_payload["acceptance_evidence"] is False
    assert latest_payload["checklist_gate"] == "12A"
    assert latest_payload["evidence_state"] == "route_unblock_diagnostic"


def test_run_v1_trace_terminates_cleanup_hang_after_status_is_written(
    monkeypatch,
    tmp_path: Path,
) -> None:
    runtime_config_path = tmp_path / "runtime_v1_config.json"
    runtime_cfg = {
        "paths": {
            "output_dir": str(tmp_path / "v1_output"),
        }
    }
    runtime_config_path.write_text(json.dumps(runtime_cfg), encoding="utf-8")
    run_dir = Path(runtime_cfg["paths"]["output_dir"]) / "cleanup_hang_v1"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "route_trace.jsonl").write_text('{"route":"co2","action":"route_baseline"}\n', encoding="utf-8")
    (run_dir / "route_trace_status.json").write_text(
        json.dumps(
            {
                "ok": True,
                "status_phase": "completed",
                "status_error": None,
                "trace_expected_but_missing": False,
            }
        ),
        encoding="utf-8",
    )

    class _FakeProc:
        def __init__(self) -> None:
            self.returncode = None
            self.terminated = False

        def poll(self):
            return self.returncode

        def terminate(self):
            self.terminated = True
            self.returncode = -15

        def wait(self, timeout=None):
            if self.returncode is None:
                self.returncode = -15
            return self.returncode

        def kill(self):
            self.returncode = -9

    fake_proc = _FakeProc()
    monkeypatch.setattr(compare_v1_v2_control_flow.subprocess, "Popen", lambda *args, **kwargs: fake_proc)
    monkeypatch.setattr(compare_v1_v2_control_flow, "V1_TRACE_SUBPROCESS_POLL_S", 0.0)
    monkeypatch.setattr(compare_v1_v2_control_flow, "V1_TRACE_SUBPROCESS_GRACE_S", 0.0)
    monkeypatch.setattr(compare_v1_v2_control_flow, "V1_TRACE_SUBPROCESS_TERMINATE_WAIT_S", 0.01)

    result = compare_v1_v2_control_flow._run_v1_trace(
        runtime_config_path,
        temp_c=20.0,
        h2o_only=False,
        skip_connect_check=True,
        run_id="cleanup_hang_v1",
    )

    assert result["ok"] is True
    assert result["cleanup_terminated"] is True
    assert result["cleanup_termination_reason"] == "post_run_cleanup_timeout"
    assert fake_proc.terminated is True


