import csv
import hashlib
import json
import math
from pathlib import Path

import pytest

from gas_calibrator.tools.export_v1_5_historical_fit_profile_parity import main
from gas_calibrator.validation.v1_5_algorithm_mature_queue_inputs import (
    write_v1_5_algorithm_mature_queue_inputs,
)
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_historical_fit_profile_parity import (
    build_v1_5_historical_fit_profile_parity,
)

ROOT = Path(__file__).resolve().parents[1]
PROFILE = ROOT / "configs" / "v1_5_algorithm_route_profiles.json"


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _read_csv(path: str | Path) -> list[dict[str, str]]:
    with Path(path).open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, rows: list[dict]) -> Path:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _fixture(tmp_path: Path, profile_id: str) -> dict:
    queue_model = write_v1_5_algorithm_mature_queue_inputs(
        profile_path=PROFILE,
        profile_id=profile_id,
        output_dir=tmp_path / "queues",
    )
    profile_sha = queue_model["profile_sha256"]
    absorption = profile_id == "absorption_ratio_shadow"
    co2_queue_rows = _read_csv(queue_model["co2_queue_csv"])
    h2o_queue_rows = _read_csv(queue_model["h2o_queue_csv"])
    r0_paths: dict[str, Path] = {}
    for component, variable in (("co2", "R0_CO2(T)"), ("h2o", "R0_H2O(T)")):
        if absorption:
            if component == "co2":
                temperatures = sorted({float(row["temp_c"]) + 0.2 for row in co2_queue_rows})
                evaluated_points = [
                    {"temperature_c": temp, "r0_value": 1.0 + (temp - 0.2) * 0.0001}
                    for temp in temperatures
                ]
            else:
                temperatures = sorted(
                    {float(row["temp_c"]) + 0.3 for row in h2o_queue_rows}
                    | {-20.0, 0.0, 20.0}
                )
                evaluated_points = [
                    {
                        "temperature_c": temp,
                        "r0_value": (
                            1.001 + (temp - 0.3) * 0.0001
                            if temp not in {-20.0, 0.0, 20.0}
                            else 1.001 + temp * 0.0001
                        ),
                    }
                    for temp in temperatures
                ]
            r0_paths[component] = _write_json(
                tmp_path / f"{component}_r0_model.json",
                {
                    "profile_id": profile_id,
                    "profile_sha256": profile_sha,
                    "component": component,
                    "model_variable": variable,
                    "status": "pass",
                    "model": {"kind": "test_polynomial", "coefficients": [1.0, 0.0001]},
                    "evaluated_points": evaluated_points,
                    "opens_com_ports": False,
                    "writes_coefficients": False,
                    "connects_postgresql": False,
                },
            )
    lineage = _write_json(
        tmp_path / "v1_5_algorithm_profile_lineage_gate.json",
        {
            "overall_status": "pass",
            "fit_input_allowed": True,
            "fit_input_contract": {
                "profile_id": profile_id,
                "profile_sha256": profile_sha,
                "algorithm_mode": queue_model["algorithm_mode"],
                "co2_fit_input": (
                    "A_CO2=-ln(R_CO2/R0_CO2(T))/(P_kPa/100)"
                    if absorption
                    else "R_CO2_with_chamber_temperature_terms"
                ),
                "h2o_fit_input": (
                    "A_H2O=-ln(R_H2O/R0_H2O(T))/(P_kPa/100)"
                    if absorption
                    else "R_H2O_with_chamber_temperature_terms"
                ),
                "r0_required": absorption,
                "temperature_source": "per_analyzer_chamber_T1",
                "co2_zero_and_h2o_dry_anchor_are_separate": True,
            },
            "source_paths": {
                "queue_inputs_json": queue_model["manifest_json"],
                "co2_r0_model_json": str(r0_paths.get("co2", "")),
                "h2o_r0_model_json": str(r0_paths.get("h2o", "")),
            },
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
            "connects_postgresql": False,
        },
    )

    common = {
        "device_id": "001",
        "profile_id": profile_id,
        "profile_sha256": profile_sha,
        "algorithm_mode": queue_model["algorithm_mode"],
        "fitting_baseline": "0613",
        "route_baseline": "0620",
        "fit_eligible": "true",
        "quality_grade": "A_calibration_eligible",
        "pressure_kpa": 100.0,
        "source_route": "",
        "source_nominal_ppm": "",
        "dewpoint_c": "",
    }
    rows: list[dict] = []
    for index, queue_row in enumerate(co2_queue_rows, start=1):
        temp = float(queue_row["temp_c"])
        ppm = float(queue_row["source_nominal_ppm"])
        ratio = 0.997 - ppm * 0.00001 + temp * 0.000001
        r0 = 1.0 + temp * 0.0001
        fit_value = -math.log(ratio / r0) if absorption else ratio
        rows.append(
            {
                **common,
                "component": "co2",
                "point_id": f"co2_{index:03d}",
                "sample_role": "co2_zero_gas" if ppm == 0 else "co2_span",
                "temp_c": temp,
                "source_nominal_ppm": ppm,
                "hgen_temp_c": "",
                "hgen_rh_pct": "",
                "temperature_c": temp + 0.2,
                "ratio_r": ratio,
                "fit_input_variable": "A" if absorption else "R",
                "fit_input_value": fit_value,
                "r0_value": r0 if absorption else "",
                "r0_model_sha256": _sha(r0_paths["co2"]) if absorption else "",
            }
        )
    for index, queue_row in enumerate(h2o_queue_rows, start=1):
        temp = float(queue_row["temp_c"])
        hgen_temp = float(queue_row["hgen_temp_c"])
        rh = float(queue_row["hgen_rh_pct"])
        ratio = 0.998 - rh * 0.00002 + temp * 0.000001
        r0 = 1.001 + temp * 0.0001
        fit_value = -math.log(ratio / r0) if absorption else ratio
        rows.append(
            {
                **common,
                "component": "h2o",
                "point_id": f"h2o_{index:03d}",
                "sample_role": "h2o_wet",
                "temp_c": temp,
                "source_nominal_ppm": "",
                "hgen_temp_c": hgen_temp,
                "hgen_rh_pct": rh,
                "temperature_c": temp + 0.3,
                "ratio_r": ratio,
                "fit_input_variable": "A" if absorption else "R",
                "fit_input_value": fit_value,
                "r0_value": r0 if absorption else "",
                "r0_model_sha256": _sha(r0_paths["h2o"]) if absorption else "",
            }
        )
    for index, temp in enumerate((-20.0, 0.0, 20.0), start=1):
        ratio = 0.999 + temp * 0.000001
        r0 = 1.001 + temp * 0.0001
        fit_value = -math.log(ratio / r0) if absorption else ratio
        rows.append(
            {
                **common,
                "component": "h2o",
                "point_id": f"h2o_dry_anchor_{index}",
                "sample_role": "h2o_dry_gas_anchor",
                "temp_c": "",
                "source_route": "co2_zero_gas",
                "source_nominal_ppm": 0,
                "hgen_temp_c": "",
                "hgen_rh_pct": "",
                "temperature_c": temp,
                "dewpoint_c": -35.0 + index,
                "ratio_r": ratio,
                "fit_input_variable": "A" if absorption else "R",
                "fit_input_value": fit_value,
                "r0_value": r0 if absorption else "",
                "r0_model_sha256": _sha(r0_paths["h2o"]) if absorption else "",
            }
        )
    points = _write_csv(tmp_path / "historical_fit_points.csv", rows)
    return {"lineage": lineage, "points": points, "rows": rows}


@pytest.mark.parametrize(
    ("profile_id", "co2_count", "h2o_count", "fit_variable"),
    [
        ("legacy_ratio_production", 45, 13, "R"),
        ("absorption_ratio_shadow", 47, 14, "A"),
    ],
)
def test_historical_fit_profile_parity_passes_complete_profile_specific_rows(
    tmp_path: Path, profile_id: str, co2_count: int, h2o_count: int, fit_variable: str
) -> None:
    fixture = _fixture(tmp_path, profile_id)
    model = build_v1_5_historical_fit_profile_parity(
        algorithm_profile_lineage_json=fixture["lineage"],
        fit_points_csv=fixture["points"],
    )
    device = model["device_summaries"][0]
    assert model["overall_status"] == "pass"
    assert model["historical_fit_replay_allowed"] is True
    assert model["fit_input_variable"] == fit_variable
    assert device["co2_observed_count"] == co2_count
    assert device["h2o_wet_observed_count"] == h2o_count
    assert device["h2o_dry_anchor_temperature_count"] == 3
    assert model["opens_com_ports"] is False
    assert model["writes_coefficients"] is False
    assert model["not_real_acceptance_evidence"] is True


def test_legacy_profile_rejects_absorption_or_r0_reuse(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, "legacy_ratio_production")
    fixture["rows"][0]["fit_input_variable"] = "A"
    fixture["rows"][0]["r0_value"] = 1.0
    _write_csv(fixture["points"], fixture["rows"])
    model = build_v1_5_historical_fit_profile_parity(
        algorithm_profile_lineage_json=fixture["lineage"], fit_points_csv=fixture["points"]
    )
    reasons = {reason for row in model["replay_rows"] for reason in row["reasons"]}
    assert model["overall_status"] == "blocked"
    assert "legacy_profile_requires_R" in reasons
    assert "legacy_profile_must_not_consume_R0" in reasons


@pytest.mark.parametrize("tamper", ("missing_r0_hash", "wrong_r0_value", "wrong_absorption_value"))
def test_absorption_profile_rejects_missing_r0_or_wrong_A(tmp_path: Path, tamper: str) -> None:
    fixture = _fixture(tmp_path, "absorption_ratio_shadow")
    if tamper == "missing_r0_hash":
        fixture["rows"][0]["r0_model_sha256"] = ""
    elif tamper == "wrong_r0_value":
        fixture["rows"][0]["r0_value"] = float(fixture["rows"][0]["r0_value"]) + 0.01
    else:
        fixture["rows"][0]["fit_input_value"] = 123.0
    _write_csv(fixture["points"], fixture["rows"])
    model = build_v1_5_historical_fit_profile_parity(
        algorithm_profile_lineage_json=fixture["lineage"], fit_points_csv=fixture["points"]
    )
    reasons = {reason for row in model["replay_rows"] for reason in row["reasons"]}
    assert model["overall_status"] == "blocked"
    expected_reason = {
        "missing_r0_hash": "co2_r0_model_sha_mismatch",
        "wrong_r0_value": "co2_r0_value_not_from_reviewed_model",
        "wrong_absorption_value": "absorption_fit_input_value_mismatch",
    }[tamper]
    assert expected_reason in reasons


def test_missing_formal_point_or_dry_anchor_temperature_blocks(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, "legacy_ratio_production")
    rows = [row for row in fixture["rows"] if row["point_id"] not in {"co2_001", "h2o_dry_anchor_3"}]
    _write_csv(fixture["points"], rows)
    model = build_v1_5_historical_fit_profile_parity(
        algorithm_profile_lineage_json=fixture["lineage"], fit_points_csv=fixture["points"]
    )
    reasons = {reason for device in model["device_summaries"] for reason in device["reasons"]}
    assert model["overall_status"] == "blocked"
    assert "co2_expected_points_missing" in reasons
    assert "h2o_dry_gas_anchor_temperature_coverage_insufficient" in reasons


def test_0624_or_migrated_route_baseline_is_rejected(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, "legacy_ratio_production")
    fixture["rows"][0]["route_baseline"] = "0624"
    _write_csv(fixture["points"], fixture["rows"])
    model = build_v1_5_historical_fit_profile_parity(
        algorithm_profile_lineage_json=fixture["lineage"], fit_points_csv=fixture["points"]
    )
    reasons = {reason for row in model["replay_rows"] for reason in row["reasons"]}
    assert model["overall_status"] == "blocked"
    assert "route_baseline_must_be_0620_or_0621" in reasons


def test_queue_mutation_after_lineage_review_is_rejected(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, "legacy_ratio_production")
    lineage = json.loads(fixture["lineage"].read_text(encoding="utf-8"))
    queue_inputs_path = Path(lineage["source_paths"]["queue_inputs_json"])
    queue_inputs = json.loads(queue_inputs_path.read_text(encoding="utf-8"))
    Path(queue_inputs["co2_queue_csv"]).write_text("tampered\n", encoding="utf-8")
    model = build_v1_5_historical_fit_profile_parity(
        algorithm_profile_lineage_json=fixture["lineage"], fit_points_csv=fixture["points"]
    )
    reasons = {reason for check in model["checks"] for reason in check["reasons"]}
    assert model["overall_status"] == "blocked"
    assert "co2_queue_sha_mismatch_after_lineage_review" in reasons


def test_cli_fails_closed_and_inventory_marks_it_offline(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, "legacy_ratio_production")
    fixture["rows"][0]["profile_id"] = "absorption_ratio_shadow"
    _write_csv(fixture["points"], fixture["rows"])
    assert (
        main(
            [
                "--algorithm-profile-lineage-json",
                str(fixture["lineage"]),
                "--fit-points-csv",
                str(fixture["points"]),
                "--output-dir",
                str(tmp_path / "out"),
                "--fail-on-blocker",
            ]
        )
        == 2
    )
    entry = classify_v1_5_entrypoint(
        ROOT / "src/gas_calibrator/tools/export_v1_5_historical_fit_profile_parity.py",
        root=ROOT,
    )
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
