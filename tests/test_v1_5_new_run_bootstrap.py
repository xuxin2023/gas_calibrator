import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

import gas_calibrator.validation.v1_5_new_run_bootstrap as bootstrap_module
from gas_calibrator.tools.run_v1_5_new_run_bootstrap import main
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_formal_flow_contract import (
    OUT_OF_BAND_OFFLINE_STATE_ADVANCE_MODULES,
    V1_5_NEW_RUN_BOOTSTRAP_MODULE,
)
from gas_calibrator.validation.v1_5_new_run_bootstrap import (
    READY_STATUS,
    bootstrap_v1_5_new_run,
)

ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 7, 12, 12, 0, tzinfo=UTC)
RUN_ID = "v1_5_batch_20260712_001"


def _config(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "site": "bootstrap-test",
                "devices": [],
                "analyzer_mode2_init": {"command_gap_s": 1.0},
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _bootstrap(tmp_path: Path, **kwargs) -> dict:
    config = kwargs.pop("config_path", _config(tmp_path / "input" / "config.json"))
    return bootstrap_v1_5_new_run(
        config_path=config,
        runs_root=tmp_path / "runs",
        run_id=kwargs.pop("run_id", RUN_ID),
        operator=kwargs.pop("operator", "operator-a"),
        reviewer=kwargs.pop("reviewer", "reviewer-b"),
        approver=kwargs.pop("approver", "approver-c"),
        now=NOW,
        **kwargs,
    )


def test_bootstrap_creates_atomic_zero_authority_plan_and_state(tmp_path: Path) -> None:
    model = _bootstrap(tmp_path)
    run_root = Path(model["run_root"])
    plan = json.loads(
        (run_root / "v1_5_full_flow_plan.json").read_text(encoding="utf-8")
    )
    state = json.loads(
        (run_root / "v1_5_full_flow_state.json").read_text(encoding="utf-8")
    )
    assert model["overall_status"] == READY_STATUS
    assert model["new_run_bootstrap_ready"] is True
    assert model["run_id"] == RUN_ID
    assert model["completed_step_ids"] == []
    assert model["failed_step_ids"] == []
    assert model["physical_capabilities"] == {
        "allow_real_com": False,
        "allow_pressure_control": False,
        "allow_route_control": False,
        "allow_writes": False,
    }
    assert model["execution_supported"] is False
    assert model["opens_com_ports"] is False
    assert model["connects_postgresql"] is False
    assert plan["run_id"] == RUN_ID
    assert plan["dry_run_only"] is True
    assert state["run_id"] == RUN_ID
    assert state["current_step_id"] == plan["steps"][0]["step_id"]
    assert state["completed_step_ids"] == []
    assert state["failed_step_ids"] == []
    assert state["allow_real_com"] is False
    assert state["allow_pressure_control"] is False
    assert state["allow_route_control"] is False
    assert state["allow_writes"] is False
    assert not list((tmp_path / "runs").glob(f".{RUN_ID}.bootstrap.*"))
    assert not (tmp_path / "runs" / f".{RUN_ID}.bootstrap.lock").exists()


def test_bootstrap_snapshots_exact_config_and_hashes_artifacts(tmp_path: Path) -> None:
    config = _config(tmp_path / "input" / "config.json")
    model = _bootstrap(tmp_path, config_path=config)
    snapshot = Path(model["config_snapshot_json"])
    plan = Path(model["full_flow_plan_json"])
    state = Path(model["authoritative_state_json"])
    assert snapshot.read_bytes() == config.read_bytes()
    assert model["config_source_sha256"] == _sha(config)
    assert model["config_snapshot_sha256"] == _sha(snapshot)
    assert model["full_flow_plan_sha256"] == _sha(plan)
    assert model["authoritative_state_sha256"] == _sha(state)
    assert Path(model["manifest_json"]).is_file()
    assert model["manifest_sha256"] == _sha(Path(model["manifest_json"]))
    assert Path(model["artifact_csv"]).is_file()


def test_bootstrap_keeps_mature_route_modules_in_generated_plan(tmp_path: Path) -> None:
    model = _bootstrap(tmp_path)
    plan = json.loads(Path(model["full_flow_plan_json"]).read_text(encoding="utf-8"))
    by_id = {row["step_id"]: row for row in plan["steps"]}
    assert by_id["co2_open_flow_sampling"]["tool_module"] == (
        "gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue"
    )
    assert by_id["h2o_open_flow_sampling"]["tool_module"] == (
        "gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_queue"
    )


def test_bootstrap_refuses_existing_run_without_changing_it(tmp_path: Path) -> None:
    first = _bootstrap(tmp_path)
    manifest = Path(first["manifest_json"])
    before = _sha(manifest)
    with pytest.raises(FileExistsError, match="already exists"):
        _bootstrap(tmp_path)
    assert _sha(manifest) == before


def test_bootstrap_refuses_concurrent_run_lock_before_building(tmp_path: Path) -> None:
    config = _config(tmp_path / "input" / "config.json")
    runs = tmp_path / "runs"
    runs.mkdir()
    lock = runs / f".{RUN_ID}.bootstrap.lock"
    lock.write_text("active", encoding="utf-8")
    with pytest.raises(FileExistsError, match="bootstrap already active"):
        bootstrap_v1_5_new_run(
            config_path=config,
            runs_root=runs,
            run_id=RUN_ID,
            operator="operator-a",
            reviewer="reviewer-b",
            approver="approver-c",
            now=NOW,
        )
    assert not (runs / RUN_ID).exists()
    assert lock.read_text(encoding="utf-8") == "active"


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"run_id": "bad id"}, "run_id"),
        ({"reviewer": "approver-c"}, "must be present and distinct"),
    ],
)
def test_bootstrap_rejects_invalid_run_or_identities_before_target(
    tmp_path: Path, kwargs: dict, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        _bootstrap(tmp_path, **kwargs)
    runs = tmp_path / "runs"
    assert not runs.exists() or not any(runs.iterdir())


def test_bootstrap_rejects_config_drift_before_atomic_publish(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path / "input" / "config.json")
    original = bootstrap_module.write_full_flow_state

    def drift_after_state(*args, **kwargs):
        result = original(*args, **kwargs)
        config.write_text('{"changed": true}\n', encoding="utf-8")
        return result

    monkeypatch.setattr(bootstrap_module, "write_full_flow_state", drift_after_state)
    with pytest.raises(RuntimeError, match="config changed"):
        _bootstrap(tmp_path, config_path=config)
    assert not (tmp_path / "runs" / RUN_ID).exists()
    assert not list((tmp_path / "runs").glob(f".{RUN_ID}.bootstrap.*"))
    assert not (tmp_path / "runs" / f".{RUN_ID}.bootstrap.lock").exists()


def test_bootstrap_rejects_reparse_run_root_before_writing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path / "input" / "config.json")
    runs_root = (tmp_path / "runs").absolute()
    original = bootstrap_module._contains_reparse
    monkeypatch.setattr(
        bootstrap_module,
        "_contains_reparse",
        lambda path: Path(path).absolute() == runs_root or original(path),
    )
    with pytest.raises(ValueError, match="must not contain a reparse point"):
        bootstrap_v1_5_new_run(
            config_path=config,
            runs_root=runs_root,
            run_id=RUN_ID,
            operator="operator-a",
            reviewer="reviewer-b",
            approver="approver-c",
            now=NOW,
        )
    assert not runs_root.exists()


def test_bootstrap_cli_rejects_execution_or_manual_prefix_flags(tmp_path: Path) -> None:
    config = _config(tmp_path / "input" / "config.json")
    for forbidden in ("--execute", "--allow-real-com", "--completed-step"):
        output_root = tmp_path / forbidden.removeprefix("--")
        with pytest.raises(SystemExit) as exc_info:
            main(
                [
                    "--config",
                    str(config),
                    "--runs-root",
                    str(output_root),
                    "--run-id",
                    RUN_ID,
                    "--operator",
                    "operator-a",
                    "--reviewer",
                    "reviewer-b",
                    "--approver",
                    "approver-c",
                    forbidden,
                ]
            )
        assert exc_info.value.code == 2
        assert not output_root.exists()


def test_bootstrap_is_offline_manual_launcher_and_not_a_canonical_stage() -> None:
    path = ROOT / "src/gas_calibrator/tools/run_v1_5_new_run_bootstrap.py"
    entry = classify_v1_5_entrypoint(path, root=ROOT)
    assert entry.category == "full_flow_orchestration"
    assert entry.formal_status == "manual_new_batch_bootstrap_no_com"
    assert entry.risk_level == "state_file_write_risk"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert V1_5_NEW_RUN_BOOTSTRAP_MODULE in OUT_OF_BAND_OFFLINE_STATE_ADVANCE_MODULES
