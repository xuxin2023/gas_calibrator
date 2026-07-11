import csv
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_batch_initialization_closeout_index import main as cli_main
from gas_calibrator.validation.v1_5_batch_initialization_closeout_index import (
    READY_STATUS,
    REVIEW_STATUS,
    ROUTE_PENDING_STATUS,
    SCHEMA,
    build_v1_5_batch_initialization_closeout_index,
    write_v1_5_batch_initialization_closeout_index,
)
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint


ROOT = Path(__file__).resolve().parents[1]


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _getco_snapshot() -> dict:
    getco = {f"GETCO{index}": {"raw": f"<C0:{index}>", "values": [float(index)]} for index in range(1, 10)}
    getco["GETCO5"] = {"raw": "<C0:0,C1:1>", "values": [0.0, 1.0]}
    getco["GETCO6"] = {"raw": "<C0:0,C1:1>", "values": [0.0, 1.0]}
    getco["GETCO7"] = {"raw": "<C0:0,C1:1,C2:0,C3:0>", "values": [0.0, 1.0, 0.0, 0.0]}
    getco["GETCO8"] = {"raw": "<C0:0,C1:1,C2:0,C3:0>", "values": [0.0, 1.0, 0.0, 0.0]}
    getco["GETCO9"] = {"raw": "<C0:0,C1:1,C2:0,C3:0>", "values": [0.0, 1.0, 0.0, 0.0]}
    return getco


def _readonly_payload(count: int = 6) -> dict:
    snapshots = []
    for index in range(1, count + 1):
        sn = f"012607{index:02d}"
        snapshots.append(
            {
                "ga_label": f"GA{index:02d}",
                "port": f"COM{34 + index}",
                "protocol_device_id_expected": f"{index:03d}",
                "sn_code_expected": sn,
                "sn_code_read": sn,
                "device_code": sn,
                "algorithm": "legacy_ratio",
                "getco": _getco_snapshot(),
                "runtime_evidence": {
                    "mode": 2,
                    "ftd_hz": 1.0,
                    "average1": "AVERAGE1",
                    "average2": "AVERAGE2",
                },
                "check_monitor_raw": "",
                "holds": [],
            }
        )
    return {
        "schema": "v1_5_formal_readonly_com_minimal_executor_v1",
        "overall_status": "readonly_com_minimal_executor_completed_no_write",
        "writes_sn": False,
        "writes_device_id": False,
        "writes_coefficients": False,
        "connects_postgresql": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "database_written": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
        "identity_getco_snapshots": snapshots,
    }


def _pressure_payload(count: int = 6) -> dict:
    return {
        "overall_status": "ready_for_open_flow_main_calibration",
        "devices": [
            {
                "protocol_device_id": f"{index:03d}",
                "readiness_status": "pass",
                "senco9_write_status": "written_readback_verified",
            }
            for index in range(1, count + 1)
        ],
    }


def _route_payload() -> dict:
    return {"overall_status": "pass", "route": "mature_0620_0621_clean"}


def test_batch_initialization_closeout_index_can_bind_six_legacy_devices(tmp_path: Path) -> None:
    readonly_path = _write_json(tmp_path / "readonly.json", _readonly_payload())
    pressure_path = _write_json(tmp_path / "pressure.json", _pressure_payload())
    route_path = _write_json(tmp_path / "route.json", _route_payload())

    model = build_v1_5_batch_initialization_closeout_index(
        readonly_com_executor_json=readonly_path,
        pressure_readiness_json=pressure_path,
        route_readiness_json=route_path,
    )

    assert model["schema"] == SCHEMA
    assert model["overall_status"] == READY_STATUS
    assert model["batch_initialization_closeout_ready"] is True
    assert model["ready_for_mature_open_flow_from_initialization_index"] is True
    assert model["device_count"] == 6
    assert model["device_ready_count"] == 6
    assert model["legacy_point_counts"] == {"co2": 45, "h2o": 13}
    assert model["new_algorithm_profile_point_counts"] == {"co2": 47, "h2o": 14}
    assert all(row["ready_for_pre_gas_index"] for row in model["device_rows"])
    assert all(row["algorithm"] == "legacy_ratio" for row in model["device_rows"])
    assert all(row["check_scope_ready"] is True for row in model["device_rows"])
    assert all(row["status"] == "pass" for row in model["gate_rows"])
    assert model["opens_com_ports"] is False
    assert model["read_only_real_com_execution_allowed"] is False
    assert model["writes_sn"] is False
    assert model["writes_coefficients"] is False
    assert model["connects_postgresql"] is False
    assert model["controls_water_or_gas_routes"] is False
    assert model["formal_release_allowed"] is False
    assert model["database_import_allowed"] is False


def test_batch_initialization_closeout_waits_for_route_readiness(tmp_path: Path) -> None:
    readonly_path = _write_json(tmp_path / "readonly.json", _readonly_payload())
    pressure_path = _write_json(tmp_path / "pressure.json", _pressure_payload())

    model = build_v1_5_batch_initialization_closeout_index(
        readonly_com_executor_json=readonly_path,
        pressure_readiness_json=pressure_path,
    )

    assert model["overall_status"] == ROUTE_PENDING_STATUS
    assert model["batch_initialization_closeout_ready"] is True
    assert model["ready_for_route_readiness_gate"] is True
    assert model["ready_for_mature_open_flow_from_initialization_index"] is False
    assert "route_readiness_evidence_missing" in model["review_reasons"]


def test_batch_initialization_closeout_requires_per_device_pressure_rows(tmp_path: Path) -> None:
    readonly_path = _write_json(tmp_path / "readonly.json", _readonly_payload())
    pressure_path = _write_json(
        tmp_path / "pressure.json",
        {"overall_status": "ready_for_open_flow_main_calibration"},
    )
    route_path = _write_json(tmp_path / "route.json", _route_payload())

    model = build_v1_5_batch_initialization_closeout_index(
        readonly_com_executor_json=readonly_path,
        pressure_readiness_json=pressure_path,
        route_readiness_json=route_path,
    )

    assert model["overall_status"] == REVIEW_STATUS
    assert model["batch_initialization_closeout_ready"] is False
    assert model["ready_for_mature_open_flow_from_initialization_index"] is False
    assert "pressure_s9_per_device_rows_missing" in model["review_reasons"]
    assert any(
        "s9_pressure_readiness_missing_or_not_ready" in reason
        for reason in model["review_reasons"]
    )


def test_batch_initialization_closeout_blocks_duplicate_sn_and_non_neutral_auxiliary(tmp_path: Path) -> None:
    readonly = _readonly_payload()
    readonly["identity_getco_snapshots"][1]["sn_code_read"] = "01260701"
    readonly["identity_getco_snapshots"][1]["device_code"] = "01260701"
    readonly["identity_getco_snapshots"][2]["getco"]["GETCO5"]["values"] = [0.2, 1.0]
    readonly_path = _write_json(tmp_path / "readonly.json", readonly)
    pressure_path = _write_json(tmp_path / "pressure.json", _pressure_payload())
    route_path = _write_json(tmp_path / "route.json", _route_payload())

    model = build_v1_5_batch_initialization_closeout_index(
        readonly_com_executor_json=readonly_path,
        pressure_readiness_json=pressure_path,
        route_readiness_json=route_path,
    )

    assert model["overall_status"] == REVIEW_STATUS
    assert model["batch_initialization_closeout_ready"] is False
    assert model["ready_for_mature_open_flow_from_initialization_index"] is False
    assert any("duplicate_sn_code:01260701" in reason for reason in model["review_reasons"])
    assert any("s5_getco5_not_neutral" in reason for reason in model["review_reasons"])
    assert model["writes_coefficients"] is False


def test_batch_initialization_closeout_writer_cli_and_entrypoint(tmp_path: Path) -> None:
    readonly_path = _write_json(tmp_path / "readonly.json", _readonly_payload())
    pressure_path = _write_json(tmp_path / "pressure.json", _pressure_payload())
    route_path = _write_json(tmp_path / "route.json", _route_payload())
    output_dir = tmp_path / "index"

    paths = write_v1_5_batch_initialization_closeout_index(
        output_dir=output_dir,
        readonly_com_executor_json=readonly_path,
        pressure_readiness_json=pressure_path,
        route_readiness_json=route_path,
    )

    assert paths["manifest"].exists()
    assert paths["devices"].exists()
    assert paths["gates"].exists()
    assert paths["markdown"].exists()
    model = json.loads(paths["manifest"].read_text(encoding="utf-8"))
    assert model["overall_status"] == READY_STATUS
    with paths["devices"].open("r", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 6
    assert rows[0]["sn_code"] == "01260701"
    assert "ready_for_mature_open_flow_from_initialization_index" in paths["markdown"].read_text(encoding="utf-8")

    cli_dir = tmp_path / "cli"
    assert (
        cli_main(
            [
                "--output-dir",
                str(cli_dir),
                "--readonly-com-executor-json",
                str(readonly_path),
                "--pressure-readiness-json",
                str(pressure_path),
                "--route-readiness-json",
                str(route_path),
                "--fail-on-review-required",
            ]
        )
        == 0
    )
    assert (cli_dir / "v1_5_batch_initialization_closeout_index.json").exists()

    entry = classify_v1_5_entrypoint(
        ROOT / "src/gas_calibrator/tools/export_v1_5_batch_initialization_closeout_index.py",
        root=ROOT,
    )
    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
    assert any("batch initialization closeout index" in note for note in entry.notes)


def test_batch_initialization_closeout_cli_fails_closed_before_route_readiness(tmp_path: Path) -> None:
    readonly_path = _write_json(tmp_path / "readonly.json", _readonly_payload())
    pressure_path = _write_json(tmp_path / "pressure.json", _pressure_payload())
    output_dir = tmp_path / "blocked_index"

    assert (
        cli_main(
            [
                "--output-dir",
                str(output_dir),
                "--readonly-com-executor-json",
                str(readonly_path),
                "--pressure-readiness-json",
                str(pressure_path),
                "--fail-on-review-required",
            ]
        )
        == 2
    )
    payload = json.loads(
        (output_dir / "v1_5_batch_initialization_closeout_index.json").read_text(encoding="utf-8")
    )
    assert payload["overall_status"] == ROUTE_PENDING_STATUS
    assert payload["ready_for_mature_open_flow_from_initialization_index"] is False
