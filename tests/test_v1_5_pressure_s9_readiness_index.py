import csv
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_pressure_s9_readiness_index import main as cli_main
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_pressure_s9_readiness_index import (
    READY_STATUS,
    REVIEW_STATUS,
    SCHEMA,
    build_v1_5_pressure_s9_readiness_index,
    write_v1_5_pressure_s9_readiness_index,
)


ROOT = Path(__file__).resolve().parents[1]


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _write_csv(path: Path, rows: list[dict]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    keys: list[str] = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _fit_rows() -> list[dict]:
    rows = []
    for index in range(1, 7):
        row = {
            "ga_label": f"GA{index:02d}",
            "port": f"COM{34 + index}",
            "protocol_device_id": f"{index:03d}",
            "sn_code": f"012607{index:02d}",
            "device_code": f"012607{index:02d}",
            "status": "pass",
            "recommendation": "review_senco9_offset_candidate_no_write",
            "s9_model": "offset_only",
            "offset_only_residual_max_abs_hpa": "0.24",
            "linear_slope_bias": "0.0003",
        }
        if index == 4:
            row.update(
                {
                    "s9_model": "linear_s9_controlled_exception",
                    "recommendation": "linear_s9_controlled_exception",
                    "linear_residual_max_abs_hpa": "0.19",
                    "linear_slope_bias": "-0.0092",
                }
            )
        rows.append(row)
    return rows


def _write_rows() -> list[dict]:
    rows = []
    for index in range(1, 7):
        row = {
            "ga_label": f"GA{index:02d}",
            "port": f"COM{34 + index}",
            "protocol_device_id": f"{index:03d}",
            "sn_code": f"012607{index:02d}",
            "device_code": f"012607{index:02d}",
            "s9_action": "controlled_senco9_write_readback_reverify",
            "s9_model": "offset_only",
            "senco9_write_status": "written_readback_verified",
            "getco9_readback_values": "[0.123, 1.0, 0.0, 0.0]",
        }
        if index == 4:
            row.update(
                {
                    "s9_action": "linear_s9_controlled_exception",
                    "s9_model": "linear_s9_controlled_exception",
                    "linear_exception_authorized": "true",
                    "exception_review_status": "approved",
                    "getco9_readback_values": "[-2.9617, 0.990751, 0.0, 0.0]",
                }
            )
        rows.append(row)
    return rows


def _reverify_rows() -> list[dict]:
    rows = []
    for index in range(1, 7):
        rows.append(
            {
                "ga_label": f"GA{index:02d}",
                "port": f"COM{34 + index}",
                "protocol_device_id": f"{index:03d}",
                "sn_code": f"012607{index:02d}",
                "device_code": f"012607{index:02d}",
                "status": "pass",
                "pressure_reverify_status": "post_write_pressure_reverify_pass",
                "max_abs_diff_hpa": "0.219" if index == 4 else "0.31",
            }
        )
    return rows


def test_pressure_s9_readiness_index_allows_offset_default_and_linear_exception(tmp_path: Path) -> None:
    model = build_v1_5_pressure_s9_readiness_index(
        no_write_fit_summary_json=None,
    )
    assert model["overall_status"] == REVIEW_STATUS

    fit_payload = {"pressure_fit_summary": _fit_rows()}
    write_payload = {"senco9_write_readback": _write_rows()}
    reverify_payload = {"pressure_reverify_rows": _reverify_rows()}

    fit_path = _write_json(tmp_path / "fit.json", fit_payload)
    write_path = _write_json(tmp_path / "write.json", write_payload)
    reverify_path = _write_json(tmp_path / "reverify.json", reverify_payload)
    model = build_v1_5_pressure_s9_readiness_index(
        no_write_fit_summary_json=fit_path,
        senco9_write_readback_json=write_path,
        pressure_reverify_json=reverify_path,
    )

    assert model["schema"] == SCHEMA
    assert model["overall_status"] == READY_STATUS
    assert model["ready_for_mature_open_flow_pressure_s9_index"] is True
    assert model["device_count"] == 6
    assert model["device_ready_count"] == 6
    assert model["linear_exception_count"] == 1
    ga04 = next(row for row in model["device_rows"] if row["protocol_device_id"] == "004")
    assert ga04["s9_model"] == "linear_s9_controlled_exception"
    assert ga04["linear_exception_authorized"] is True
    assert ga04["pressure_reverify_ready"] is True
    assert all(row["status"] == "pass" for row in model["gate_rows"])
    assert model["opens_com_ports"] is False
    assert model["controls_pressure"] is False
    assert model["controls_water_or_gas_routes"] is False
    assert model["connects_postgresql"] is False
    assert model["writes_senco9"] is False
    assert model["formal_release_allowed"] is False
    assert model["database_import_allowed"] is False


def test_pressure_s9_readiness_blocks_linear_without_explicit_exception(tmp_path: Path) -> None:
    fit_rows = _fit_rows()
    write_rows = _write_rows()
    fit_rows[3]["s9_model"] = "linear"
    fit_rows[3]["recommendation"] = "linear_model_better"
    write_rows[3]["s9_model"] = "linear"
    write_rows[3]["s9_action"] = "controlled_senco9_write_readback_reverify"
    write_rows[3]["exception_review_status"] = ""
    write_rows[3]["linear_exception_authorized"] = ""

    fit_path = _write_json(tmp_path / "fit.json", {"pressure_fit_summary": fit_rows})
    write_path = _write_json(tmp_path / "write.json", {"senco9_write_readback": write_rows})
    reverify_path = _write_json(tmp_path / "reverify.json", {"pressure_reverify_rows": _reverify_rows()})

    model = build_v1_5_pressure_s9_readiness_index(
        no_write_fit_summary_json=fit_path,
        senco9_write_readback_json=write_path,
        pressure_reverify_json=reverify_path,
    )

    assert model["overall_status"] == REVIEW_STATUS
    assert model["ready_for_mature_open_flow_pressure_s9_index"] is False
    assert any("linear_s9_model_requires_explicit_controlled_exception" in reason for reason in model["review_reasons"])
    assert model["writes_senco9"] is False


def test_pressure_s9_readiness_requires_readback_and_reverify(tmp_path: Path) -> None:
    write_rows = _write_rows()
    reverify_rows = _reverify_rows()
    write_rows[0]["getco9_readback_values"] = ""
    reverify_rows[1]["max_abs_diff_hpa"] = "0.9"

    fit_path = _write_json(tmp_path / "fit.json", {"pressure_fit_summary": _fit_rows()})
    write_path = _write_json(tmp_path / "write.json", {"senco9_write_readback": write_rows})
    reverify_path = _write_json(tmp_path / "reverify.json", {"pressure_reverify_rows": reverify_rows})

    model = build_v1_5_pressure_s9_readiness_index(
        no_write_fit_summary_json=fit_path,
        senco9_write_readback_json=write_path,
        pressure_reverify_json=reverify_path,
    )

    assert model["overall_status"] == REVIEW_STATUS
    assert any("getco9_readback_values_missing_or_short" in reason for reason in model["review_reasons"])
    assert any("pressure_reverify_max_abs_error_hpa=0.900>limit=0.500" in reason for reason in model["review_reasons"])
    assert model["device_ready_count"] == 4


def test_pressure_s9_readiness_writer_cli_and_entrypoint(tmp_path: Path) -> None:
    fit_path = _write_csv(tmp_path / "fit.csv", _fit_rows())
    write_path = _write_csv(tmp_path / "write.csv", _write_rows())
    reverify_path = _write_csv(tmp_path / "reverify.csv", _reverify_rows())

    output_dir = tmp_path / "index"
    paths = write_v1_5_pressure_s9_readiness_index(
        output_dir=output_dir,
        no_write_fit_summary_csv=fit_path,
        senco9_write_readback_csv=write_path,
        pressure_reverify_csv=reverify_path,
    )

    assert paths["manifest"].exists()
    assert paths["devices"].exists()
    assert paths["gates"].exists()
    assert paths["markdown"].exists()
    model = json.loads(paths["manifest"].read_text(encoding="utf-8"))
    assert model["overall_status"] == READY_STATUS
    assert "linear_s9_controlled_exception" in paths["markdown"].read_text(encoding="utf-8")

    cli_dir = tmp_path / "cli"
    assert (
        cli_main(
            [
                "--output-dir",
                str(cli_dir),
                "--no-write-fit-summary-csv",
                str(fit_path),
                "--senco9-write-readback-csv",
                str(write_path),
                "--pressure-reverify-csv",
                str(reverify_path),
            ]
        )
        == 0
    )
    assert (cli_dir / "v1_5_pressure_s9_readiness_index.json").exists()

    entry = classify_v1_5_entrypoint(
        ROOT / "src/gas_calibrator/tools/export_v1_5_pressure_s9_readiness_index.py",
        root=ROOT,
    )
    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
    assert any("pressure/SENCO9 readiness index" in note for note in entry.notes)
