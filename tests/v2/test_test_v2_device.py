import json
from pathlib import Path
from pathlib import PureWindowsPath
from types import SimpleNamespace

from gas_calibrator.v2.core.point_parser import PointParser
from gas_calibrator.v2.core.route_planner import RoutePlanner
from gas_calibrator.v2.entry import load_config_bundle
from gas_calibrator.v2.scripts import test_v2_device
from gas_calibrator.v2.scripts import test_v2_safe
from gas_calibrator.v2.scripts.test_v2_device import (
    BENCH_RUNTIME_PROFILE,
    CONFIG_PATH,
    MAINLINE_RUNTIME_PROFILE,
    _build_runtime_config,
)

COMPARE_FIXTURE_DIR = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "gas_calibrator"
    / "v2"
    / "output"
    / "v1_v2_compare"
)


def _compare_fixture_paths() -> list[Path]:
    return sorted(COMPARE_FIXTURE_DIR.glob("compare_collect_config*.json"))


def test_build_runtime_config_preserves_compare_missing_pressure_policy() -> None:
    config_path = COMPARE_FIXTURE_DIR / "compare_collect_config_0c_v2.json"
    _, raw_cfg, _ = load_config_bundle(str(config_path))

    runtime_cfg = _build_runtime_config(raw_cfg)

    assert runtime_cfg.workflow.missing_pressure_policy == "carry_forward"

    planner = RoutePlanner(runtime_cfg, PointParser())
    points = planner.point_parser.parse(runtime_cfg.paths.points_excel)
    sources = planner.co2_sources(points)
    planned_tags = {
        planner.co2_point_tag(planner.build_co2_pressure_point(source, pressure_point))
        for source in sources
        for pressure_point in planner.co2_pressure_points(source, points)
    }

    assert "co2_groupa_1000ppm_1100hpa" in planned_tags
    assert "co2_groupa_1000ppm_800hpa" not in planned_tags
    assert "co2_groupa_1000ppm_500hpa" not in planned_tags


def test_compare_fixture_config_uses_portable_relative_paths() -> None:
    for config_path in _compare_fixture_paths():
        payload = json.loads(config_path.read_text(encoding="utf-8"))

        assert not PureWindowsPath(payload["paths"]["points_excel"]).is_absolute()
        assert not PureWindowsPath(payload["paths"]["output_dir"]).is_absolute()
        assert "_base_dir" not in payload
        assert "_user_tuning_path" not in payload


def test_compare_fixture_config_loads_with_effective_absolute_paths() -> None:
    for config_path in _compare_fixture_paths():
        payload = json.loads(config_path.read_text(encoding="utf-8"))
        _, raw_cfg, runtime_cfg = load_config_bundle(str(config_path))
        expected_base = config_path.resolve().parent

        assert Path(raw_cfg["paths"]["points_excel"]) == (expected_base / payload["paths"]["points_excel"]).resolve()
        assert Path(raw_cfg["paths"]["output_dir"]) == (expected_base / payload["paths"]["output_dir"]).resolve()
        assert Path(runtime_cfg.paths.points_excel) == (expected_base / payload["paths"]["points_excel"]).resolve()
        assert Path(runtime_cfg.paths.output_dir) == (expected_base / payload["paths"]["output_dir"]).resolve()


def test_load_config_bundle_and_runtime_config_share_normalized_paths(tmp_path: Path) -> None:
    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    points_path = config_dir / "points.json"
    points_path.write_text(
        json.dumps({"points": [{"index": 1, "temperature": 25.0, "route": "h2o"}]}),
        encoding="utf-8",
    )
    config_path = config_dir / "app.json"
    config_path.write_text(
        json.dumps(
            {
                "devices": {"gas_analyzer": {"port": "SIM-GA1", "enabled": True}},
                "workflow": {"missing_pressure_policy": "carry_forward"},
                "paths": {"points_excel": "points.json", "output_dir": "output", "logs_dir": "logs"},
                "features": {"simulation_mode": True},
            }
        ),
        encoding="utf-8",
    )

    _, raw_cfg, config = load_config_bundle(str(config_path), simulation_mode=True)
    runtime_cfg = _build_runtime_config(raw_cfg)

    assert Path(raw_cfg["paths"]["points_excel"]) == points_path.resolve()
    assert Path(raw_cfg["paths"]["output_dir"]) == (config_dir / "output").resolve()
    assert Path(raw_cfg["paths"]["logs_dir"]) == (config_dir / "logs").resolve()
    assert Path(runtime_cfg.paths.points_excel) == points_path.resolve()
    assert Path(runtime_cfg.paths.output_dir) == (config_dir / "output").resolve()
    assert Path(runtime_cfg.paths.logs_dir) == (config_dir / "logs").resolve()
    assert config.workflow.missing_pressure_policy == "carry_forward"


def test_load_config_bundle_supports_config_dir_relative_paths() -> None:
    _, raw_cfg, config = load_config_bundle(str(CONFIG_PATH), simulation_mode=True)

    assert Path(raw_cfg["paths"]["points_excel"]) == (
        CONFIG_PATH.resolve().parents[1] / "configs" / "test_points.xlsx"
    ).resolve()
    assert Path(config.paths.points_excel) == (
        CONFIG_PATH.resolve().parents[1] / "configs" / "test_points.xlsx"
    ).resolve()
    assert Path(raw_cfg["paths"]["output_dir"]) == (
        CONFIG_PATH.resolve().parents[1] / "output" / "test_v2"
    ).resolve()
    assert Path(raw_cfg["paths"]["logs_dir"]) == (
        CONFIG_PATH.resolve().parents[1] / "logs"
    ).resolve()
    assert Path(raw_cfg["modeling"]["export"]["output_dir"]) == (
        CONFIG_PATH.resolve().parents[1] / "logs" / "modeling_offline"
    ).resolve()


def test_script_modules_are_marked_cli_only_for_pytest_collection() -> None:
    assert test_v2_device.__test__ is False
    assert test_v2_safe.__test__ is False


def test_test_v2_config_defaults_to_simulation_mode_true() -> None:
    _, raw_cfg, config = load_config_bundle(str(CONFIG_PATH))

    assert raw_cfg["features"]["simulation_mode"] is True
    assert config.features.simulation_mode is True


def test_load_runtime_forces_simulation_only_default() -> None:
    _, runtime_cfg = test_v2_device._load_runtime()

    assert runtime_cfg.features.simulation_mode is True


def test_write_report_adds_step2_evidence_boundary(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(test_v2_device, "OUTPUT_ROOT", tmp_path)
    monkeypatch.setattr(test_v2_device, "_timestamp", lambda: "20260329_000000")

    report_path = test_v2_device._write_report("connection", {"success": True, "simulation_mode": True})
    payload = json.loads(report_path.read_text(encoding="utf-8"))

    assert payload["evidence_source"] == "simulated_protocol"
    assert payload["not_real_acceptance_evidence"] is True
    assert payload["acceptance_level"] == "offline_regression"
    assert payload["promotion_state"] == "dry_run_only"


def test_write_report_backfills_config_safety_review(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(test_v2_device, "OUTPUT_ROOT", tmp_path)
    monkeypatch.setattr(test_v2_device, "_timestamp", lambda: "20260329_000001")

    report_path = test_v2_device._write_report(
        "connection",
        {
            "success": True,
            "simulation_mode": True,
            "config_safety": {
                "classification": "simulation_real_port_inventory_risk",
                "summary": "report safety",
                "execution_gate": {"status": "blocked"},
            },
        },
    )
    payload = json.loads(report_path.read_text(encoding="utf-8"))

    assert payload["config_safety"]["classification"] == "simulation_real_port_inventory_risk"
    assert payload["config_safety_review"]["execution_gate"]["status"] == "blocked"
    assert payload["config_safety_review"]["summary"]


def test_build_runtime_config_relocates_compare_paths_from_old_base(monkeypatch, tmp_path: Path) -> None:
    old_base = tmp_path / "old_repo"
    current_base = tmp_path / "gas_calibrator"
    v2_root = current_base / "src" / "gas_calibrator" / "v2"
    old_v2_root = old_base / "src" / "gas_calibrator" / "v2"
    points_path = current_base / "src" / "gas_calibrator" / "v2" / "configs" / "points.json"
    output_dir = current_base / "src" / "gas_calibrator" / "v2" / "output" / "portable"
    logs_dir = current_base / "src" / "gas_calibrator" / "v2" / "logs"
    tuning_path = current_base / "configs" / "user_tuning.json"
    old_points_path = old_v2_root / "configs" / "points.json"
    old_output_dir = old_v2_root / "output" / "portable"
    old_logs_dir = old_v2_root / "logs"
    old_tuning_path = old_base / "configs" / "user_tuning.json"
    points_path.parent.mkdir(parents=True)
    points_path.write_text(json.dumps({"points": []}), encoding="utf-8")
    tuning_path.parent.mkdir(parents=True)
    tuning_path.write_text("{}", encoding="utf-8")
    old_points_path.parent.mkdir(parents=True)
    old_points_path.write_text(json.dumps({"points": []}), encoding="utf-8")
    old_output_dir.mkdir(parents=True)
    old_logs_dir.mkdir(parents=True)
    old_tuning_path.parent.mkdir(parents=True)
    old_tuning_path.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(test_v2_device, "V2_ROOT", v2_root)
    raw_cfg = {
        "_base_dir": str(old_base),
        "paths": {
            "points_excel": str(old_points_path),
            "output_dir": str(old_output_dir),
            "logs_dir": str(old_logs_dir),
        },
        "modeling": {
            "export": {
                "output_dir": str(old_logs_dir / "modeling_offline"),
            }
        },
        "_user_tuning_path": str(old_tuning_path),
    }

    normalized = test_v2_device._normalize_portable_raw_config(raw_cfg)
    runtime_cfg = test_v2_device._build_runtime_config(raw_cfg)

    assert Path(runtime_cfg.paths.points_excel) == points_path.resolve()
    assert Path(runtime_cfg.paths.output_dir) == output_dir.resolve()
    assert Path(runtime_cfg.paths.logs_dir) == logs_dir.resolve()
    assert Path(normalized["modeling"]["export"]["output_dir"]) == (logs_dir / "modeling_offline").resolve()
    assert Path(normalized["_user_tuning_path"]) == tuning_path.resolve()


def test_run_calibration_test_uses_shared_v2_service_builder(monkeypatch, tmp_path: Path) -> None:
    _, raw_cfg, config = load_config_bundle(str(CONFIG_PATH), simulation_mode=True)
    captured: dict[str, object] = {}

    class FakeService:
        def __init__(self) -> None:
            self.is_running = False
            self.device_manager = SimpleNamespace(close_all=lambda: None)
            self._status = SimpleNamespace(
                phase=SimpleNamespace(value="completed"),
                total_points=1,
                completed_points=1,
                progress=1.0,
                message="done",
                elapsed_s=0.1,
                error=None,
                current_point=None,
            )

        def set_log_callback(self, callback) -> None:
            self._callback = callback

        def get_status(self):
            return self._status

        def start(self) -> None:
            return None

        def wait(self) -> None:
            return None

        def get_output_files(self):
            return []

        def get_results(self):
            return [object()]

    def fake_create_service(builder_raw_cfg, builder_config, *, point_filter=None, preload_points=False):
        captured["config"] = builder_config
        captured["raw_cfg"] = builder_raw_cfg
        captured["point_filter"] = point_filter
        captured["preload_points"] = preload_points
        return FakeService()

    monkeypatch.setattr(test_v2_device, "_load_runtime", lambda **kwargs: (raw_cfg, config))
    monkeypatch.setattr(test_v2_device, "_create_mainline_service", fake_create_service)
    monkeypatch.setattr(
        test_v2_device,
        "_create_bench_service",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("bench path should not be used")),
    )
    monkeypatch.setattr(test_v2_device, "_write_report", lambda name, payload: tmp_path / f"{name}.json")
    monkeypatch.setattr(test_v2_device, "_sync_summary_status", lambda output_files, status: None)
    monkeypatch.setattr(test_v2_device, "_print", lambda message: None)

    assert test_v2_device.test_single_point() is True
    assert captured["config"] is config
    assert captured["raw_cfg"] == raw_cfg
    assert captured["preload_points"] is True
    assert captured["point_filter"] is not None


def test_create_mainline_service_delegates_to_formal_builder(monkeypatch) -> None:
    _, raw_cfg, config = load_config_bundle(str(CONFIG_PATH), simulation_mode=True)
    captured: dict[str, object] = {}
    service = object()

    def fake_builder(builder_config, **kwargs):
        captured["config"] = builder_config
        captured.update(kwargs)
        return service

    monkeypatch.setattr(test_v2_device, "create_calibration_service_from_config", fake_builder)

    result = test_v2_device._create_mainline_service(raw_cfg, config, preload_points=True)

    assert result is service
    assert captured["config"] is config
    assert captured["raw_cfg"] == raw_cfg
    assert captured["preload_points"] is True
    assert captured["runtime_hooks_factory"] is None
    assert "service_cls" not in captured
    assert "service_init_kwargs" not in captured


def test_create_v2_service_remains_backward_compatible_alias(monkeypatch) -> None:
    _, raw_cfg, config = load_config_bundle(str(CONFIG_PATH), simulation_mode=True)
    captured: dict[str, object] = {}
    service = object()

    def fake_mainline(builder_raw_cfg, builder_config, *, point_filter=None, preload_points=False):
        captured["raw_cfg"] = builder_raw_cfg
        captured["config"] = builder_config
        captured["point_filter"] = point_filter
        captured["preload_points"] = preload_points
        return service

    monkeypatch.setattr(test_v2_device, "_create_mainline_service", fake_mainline)

    result = test_v2_device._create_v2_service(raw_cfg, config, preload_points=True)

    assert result is service
    assert captured["raw_cfg"] == raw_cfg
    assert captured["config"] is config
    assert captured["preload_points"] is True


def test_create_bench_service_rejects_without_dual_unlock(monkeypatch) -> None:
    _, raw_cfg, config = load_config_bundle(str(CONFIG_PATH), simulation_mode=True)
    monkeypatch.delenv(test_v2_device.REAL_BENCH_UNLOCK_ENV, raising=False)

    try:
        test_v2_device._create_bench_service(raw_cfg, config, preload_points=True)
    except test_v2_device.RealBenchLockedError as exc:
        message = str(exc)
    else:
        raise AssertionError("expected RealBenchLockedError")

    assert test_v2_device.REAL_BENCH_UNLOCK_FLAG in message
    assert test_v2_device.REAL_BENCH_UNLOCK_ENV in message


def test_create_bench_service_uses_formal_builder_with_explicit_bench_profile(monkeypatch) -> None:
    _, raw_cfg, config = load_config_bundle(str(CONFIG_PATH), simulation_mode=True)
    captured: dict[str, object] = {}
    service = object()

    def fake_profile_builder(
        builder_raw_cfg,
        builder_config,
        *,
        runtime_profile,
        point_filter=None,
        preload_points=False,
        allow_real_bench=False,
    ):
        captured["raw_cfg"] = builder_raw_cfg
        captured["config"] = builder_config
        captured["runtime_profile"] = runtime_profile
        captured["point_filter"] = point_filter
        captured["preload_points"] = preload_points
        captured["allow_real_bench"] = allow_real_bench
        return service

    monkeypatch.setattr(test_v2_device, "_create_service_for_runtime_profile", fake_profile_builder)
    monkeypatch.setenv(test_v2_device.REAL_BENCH_UNLOCK_ENV, "1")

    result = test_v2_device._create_bench_service(
        raw_cfg,
        config,
        preload_points=True,
        allow_real_bench=True,
    )

    assert result is service
    assert captured["config"] is config
    assert captured["raw_cfg"] == raw_cfg
    assert captured["preload_points"] is True
    assert captured["runtime_profile"] == BENCH_RUNTIME_PROFILE
    assert captured["allow_real_bench"] is True
    assert "service_cls" not in captured
    assert "service_init_kwargs" not in captured


def test_create_service_for_bench_profile_precomputes_policy_before_runtime_hooks(monkeypatch) -> None:
    _, raw_cfg, config = load_config_bundle(str(CONFIG_PATH), simulation_mode=True)
    captured: dict[str, object] = {}
    service = object()

    def fake_factory(runtime_profile, *, policy=None):
        captured["runtime_profile"] = runtime_profile
        captured["policy"] = policy
        return "factory"

    def fake_builder(builder_config, **kwargs):
        captured["config"] = builder_config
        captured.update(kwargs)
        return service

    monkeypatch.setattr(test_v2_device, "_runtime_hooks_factory_for_profile", fake_factory)
    monkeypatch.setattr(test_v2_device, "create_calibration_service_from_config", fake_builder)
    monkeypatch.setenv(test_v2_device.REAL_BENCH_UNLOCK_ENV, "1")

    result = test_v2_device._create_service_for_runtime_profile(
        raw_cfg,
        config,
        runtime_profile=BENCH_RUNTIME_PROFILE,
        preload_points=True,
        allow_real_bench=True,
    )

    assert result is service
    assert captured["config"] is config
    assert captured["raw_cfg"] == raw_cfg
    assert captured["runtime_profile"] == BENCH_RUNTIME_PROFILE
    assert isinstance(captured["policy"], test_v2_device.BenchRuntimePolicy)
    assert captured["runtime_hooks_factory"] == "factory"
    assert captured["preload_points"] is True


def test_runtime_hooks_factory_for_mainline_profile_is_empty() -> None:
    assert test_v2_device._runtime_hooks_factory_for_profile(MAINLINE_RUNTIME_PROFILE) is None


def test_runtime_hooks_factory_for_bench_profile_builds_thin_adapter() -> None:
    calls: dict[str, object] = {}

    class FakeStabilityChecker:
        def set_debug_callback(self, callback) -> None:
            calls["debug_callback"] = callback

    original_init = object()
    original_precheck = object()
    original_final = object()

    class FakeService:
        def __init__(self) -> None:
            self._raw_cfg = {"workflow": {}}
            self._log = lambda message: None
            self.device_manager = SimpleNamespace()
            self.orchestrator = SimpleNamespace(valve_routing_service=SimpleNamespace(apply_route_baseline_valves=lambda: None))
            self.stability_checker = FakeStabilityChecker()
            self._run_initialization = original_init
            self._run_precheck = original_precheck
            self._run_finalization = original_final

    service = FakeService()
    explicit_policy = test_v2_device.BenchRuntimePolicy(warmup_retries=7, warmup_delay_s=0.3)
    factory = test_v2_device._runtime_hooks_factory_for_profile(BENCH_RUNTIME_PROFILE, policy=explicit_policy)
    hooks = factory(service, None)

    assert isinstance(hooks, test_v2_device.BenchRuntimeAdapter)
    assert hooks.service is service
    assert hooks.policy == explicit_policy
    assert calls["debug_callback"] is service._log
    assert service._run_initialization is original_init
    assert service._run_precheck is original_precheck
    assert service._run_finalization is original_final


def test_attach_bench_runtime_adapter_remains_backward_compatible_without_method_patching() -> None:
    calls: dict[str, object] = {}

    class FakeStabilityChecker:
        def set_debug_callback(self, callback) -> None:
            calls["debug_callback"] = callback

    original_init = object()
    original_precheck = object()
    original_final = object()

    class FakeService:
        def __init__(self) -> None:
            self._raw_cfg = {"workflow": {}}
            self._log = lambda message: None
            self.device_manager = SimpleNamespace()
            self.orchestrator = SimpleNamespace(valve_routing_service=SimpleNamespace(apply_route_baseline_valves=lambda: None))
            self.stability_checker = FakeStabilityChecker()
            self._run_initialization = original_init
            self._run_precheck = original_precheck
            self._run_finalization = original_final

        def set_runtime_hooks(self, hooks) -> None:
            calls["runtime_hooks"] = hooks

    service = FakeService()

    result = test_v2_device._attach_bench_runtime_adapter(service, {"workflow": {}})

    assert result is service
    assert isinstance(calls["runtime_hooks"], test_v2_device.BenchRuntimeAdapter)
    assert calls["runtime_hooks"].policy == test_v2_device.BenchRuntimePolicy()
    assert calls["debug_callback"] is service._log
    assert service._run_initialization is original_init
    assert service._run_precheck is original_precheck
    assert service._run_finalization is original_final


def test_bench_runtime_policy_extracts_only_hook_relevant_settings() -> None:
    policy = test_v2_device._bench_runtime_policy_from_raw_cfg(
        {
            "workflow": {
                "sensor_read_retry": {
                    "retries": 5,
                    "delay_s": 0.05,
                }
            }
        }
    )

    assert policy.warmup_retries == 6
    assert policy.warmup_delay_s == 0.2
    assert policy.prepare_pressure_controller is True
    assert policy.configure_opened_analyzers is True
    assert policy.restore_valve_baseline is True


def test_device_connection_uses_shared_v2_service_builder(monkeypatch, tmp_path: Path) -> None:
    _, raw_cfg, config = load_config_bundle(str(CONFIG_PATH), simulation_mode=True)
    captured: dict[str, object] = {}
    raw_cfg = {
        **raw_cfg,
        "_config_safety": {
            "classification": "simulation_real_port_inventory_risk",
            "summary": "connection safety",
            "execution_gate": {"status": "blocked"},
        },
    }

    class FakeManager:
        def open_all(self):
            return {"gas_analyzer_0": True}

        def health_check(self):
            return {"gas_analyzer_0": True}

        def close_all(self) -> None:
            captured["closed"] = True

        def get_info(self, name: str):
            return SimpleNamespace(
                device_type="gas_analyzer",
                port="SIM-GA1",
                enabled=True,
                status=SimpleNamespace(value="open"),
                error_message=None,
            )

        _device_info = {"gas_analyzer_0": object()}

    fake_service = SimpleNamespace(device_manager=FakeManager())

    monkeypatch.setattr(test_v2_device, "_load_runtime", lambda **kwargs: (raw_cfg, config))
    monkeypatch.setattr(
        test_v2_device,
        "_create_mainline_service",
        lambda builder_raw_cfg, builder_config, **kwargs: captured.update(
            {"raw_cfg": builder_raw_cfg, "config": builder_config, **kwargs}
        )
        or fake_service,
    )
    monkeypatch.setattr(
        test_v2_device,
        "_create_bench_service",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("bench path should not be used")),
    )
    monkeypatch.setattr(test_v2_device, "_configure_opened_gas_analyzers", lambda service, log: None)
    monkeypatch.setattr(
        test_v2_device,
        "_write_report",
        lambda name, payload: captured.update({"report_name": name, "report_payload": payload}) or (tmp_path / f"{name}.json"),
    )
    monkeypatch.setattr(test_v2_device, "_print", lambda message: None)

    assert test_v2_device.test_device_connection() is True
    assert captured["raw_cfg"] == raw_cfg
    assert captured["config"] is config
    assert captured["closed"] is True
    report_payload = dict(captured["report_payload"])
    assert captured["report_name"] == "connection"
    assert report_payload["config_safety"]["classification"] == "simulation_real_port_inventory_risk"
    assert report_payload["config_safety_review"]["execution_gate"]["status"] == "blocked"


def test_configure_opened_gas_analyzers_prefers_public_analyzer_fleet_service() -> None:
    calls: list[tuple[str, object, object]] = []
    private_calls: list[tuple[str, object, object]] = []
    analyzer_fleet_service = SimpleNamespace(
        all_gas_analyzers=lambda: [("ga01", "analyzer-1", "cfg-1"), ("ga02", "analyzer-2", "cfg-2")],
        configure_gas_analyzer=lambda analyzer, *, label, cfg: calls.append((label, analyzer, cfg)),
    )
    service = SimpleNamespace(
        analyzer_fleet_service=analyzer_fleet_service,
        orchestrator=SimpleNamespace(
            _all_gas_analyzers=lambda: [("legacy", "legacy-analyzer", "legacy-cfg")],
            _configure_gas_analyzer=lambda analyzer, *, label, cfg: private_calls.append((label, analyzer, cfg)),
        ),
    )
    logs: list[str] = []

    test_v2_device._configure_opened_gas_analyzers(service, logs.append)

    assert calls == [("ga01", "analyzer-1", "cfg-1"), ("ga02", "analyzer-2", "cfg-2")]
    assert private_calls == []
    assert any("configured via AnalyzerFleetService" in message for message in logs)


def test_configure_opened_gas_analyzers_falls_back_to_orchestrator_compatibility() -> None:
    calls: list[tuple[str, object, object]] = []
    service = SimpleNamespace(
        orchestrator=SimpleNamespace(
            _all_gas_analyzers=lambda: [("ga01", "analyzer-1", "cfg-1"), ("ga02", "analyzer-2", "cfg-2")],
            _configure_gas_analyzer=lambda analyzer, *, label, cfg: calls.append((label, analyzer, cfg)),
        )
    )
    logs: list[str] = []

    test_v2_device._configure_opened_gas_analyzers(service, logs.append)

    assert calls == [("ga01", "analyzer-1", "cfg-1"), ("ga02", "analyzer-2", "cfg-2")]
    assert any("configured via AnalyzerFleetService" in message for message in logs)


def test_bench_smoke_runs_via_formal_builder(monkeypatch, tmp_path: Path) -> None:
    smoke_config = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "gas_calibrator"
        / "v2"
        / "configs"
        / "smoke_v2_minimal.json"
    )

    monkeypatch.setattr(test_v2_device, "CONFIG_PATH", smoke_config)
    monkeypatch.setattr(test_v2_device, "OUTPUT_ROOT", tmp_path)
    monkeypatch.setattr(test_v2_device, "_print", lambda message: None)

    assert test_v2_device.test_full_calibration() is True
