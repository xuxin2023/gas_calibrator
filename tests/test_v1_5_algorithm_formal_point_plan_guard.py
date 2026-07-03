import csv
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_algorithm_formal_point_plan_guard import main as cli_main
from gas_calibrator.validation.v1_5_algorithm_route_profiles import (
    build_v1_5_algorithm_formal_point_plan_guard,
    write_v1_5_algorithm_formal_point_plan_guard,
)


ROOT = Path(__file__).resolve().parents[1]
PROFILE_PATH = ROOT / "configs" / "v1_5_algorithm_route_profiles.json"


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _rows_for(tables: dict, *, profile_id: str, route_kind: str) -> list[dict]:
    return [
        row
        for row in tables["formal_point_plan"]
        if row["profile_id"] == profile_id and row["route_kind"] == route_kind
    ]


def _co2_segment(rows: list[dict], temperature_c: int) -> list[int]:
    return [
        int(row["co2_ppm"])
        for row in rows
        if int(float(row["temperature_c"])) == temperature_c
    ]


def _h2o_segment(rows: list[dict], *, temperature_c: int, hgen_temp: str) -> list[int]:
    return [
        int(row["hgen_rh_pct"])
        for row in rows
        if int(float(row["temperature_c"])) == temperature_c and row["hgen_temp"] == hgen_temp
    ]


def test_algorithm_formal_point_plan_guard_locks_legacy_and_new_algorithm_counts() -> None:
    tables = build_v1_5_algorithm_formal_point_plan_guard(PROFILE_PATH)
    manifest = tables["manifest"]
    checks = {row["check_id"]: row for row in tables["checks"]}

    assert manifest["status"] == "pass"
    assert manifest["blocker_count"] == 0
    assert manifest["no_write"] is True
    assert manifest["opens_com_ports"] is False
    assert manifest["controls_water_or_gas_routes"] is False
    assert manifest["writes_coefficients"] is False
    assert manifest["legacy_co2_formal_point_count"] == 45
    assert manifest["legacy_h2o_formal_point_count"] == 13
    assert manifest["new_algorithm_co2_formal_candidate_point_count"] == 47
    assert manifest["new_algorithm_h2o_formal_candidate_point_count"] == 14
    assert checks["legacy_default_counts_locked"]["status"] == "pass"
    assert checks["new_algorithm_candidate_counts_include_required_points"]["status"] == "pass"


def test_algorithm_formal_point_plan_inserts_new_algorithm_points_inside_temperature_segments() -> None:
    tables = build_v1_5_algorithm_formal_point_plan_guard(PROFILE_PATH)
    legacy_co2 = _rows_for(tables, profile_id="legacy_ratio_production", route_kind="co2")
    legacy_h2o = _rows_for(tables, profile_id="legacy_ratio_production", route_kind="h2o")
    new_co2 = _rows_for(tables, profile_id="absorption_ratio_shadow", route_kind="co2")
    new_h2o = _rows_for(tables, profile_id="absorption_ratio_shadow", route_kind="h2o")

    assert _co2_segment(legacy_co2, -20) == [0, 400, 1000]
    assert _co2_segment(legacy_co2, -10) == [0, 400, 1000]
    assert _co2_segment(new_co2, -20) == [0, 400, 600, 1000]
    assert _co2_segment(new_co2, -10) == [0, 400, 600, 1000]
    assert _h2o_segment(legacy_h2o, temperature_c=40, hgen_temp="HGEN30C") == [50, 70]
    assert _h2o_segment(new_h2o, temperature_c=40, hgen_temp="HGEN30C") == [30, 50, 70]

    supplemental_rows = [
        row
        for row in [*new_co2, *new_h2o]
        if row["point_role"] == "new_algorithm_required_supplemental_formal_point"
    ]
    assert {
        (row["route_kind"], row["point_key"], row["historical_missing_point_semantics"])
        for row in supplemental_rows
    } == {
        ("co2", "-20/600", "formal_required_point_not_historical_resampling"),
        ("co2", "-10/600", "formal_required_point_not_historical_resampling"),
        ("h2o", "40/30/30", "formal_required_point_not_historical_resampling"),
    }
    assert {row["do_not_modify_mature_runner"] for row in supplemental_rows} == {True}


def test_algorithm_formal_point_plan_guard_writer_and_cli_create_artifacts(tmp_path: Path) -> None:
    output = tmp_path / "guard"
    outputs = write_v1_5_algorithm_formal_point_plan_guard(PROFILE_PATH, output)

    manifest = json.loads(Path(outputs["manifest"]).read_text(encoding="utf-8-sig"))
    assert manifest["status"] == "pass"
    rows = _read_csv(output / "v1_5_algorithm_formal_point_plan.csv")
    assert len([row for row in rows if row["profile_id"] == "legacy_ratio_production" and row["route_kind"] == "co2"]) == 45
    assert len([row for row in rows if row["profile_id"] == "absorption_ratio_shadow" and row["route_kind"] == "co2"]) == 47
    assert Path(outputs["summary"]).read_text(encoding="utf-8").startswith("# V1.5 algorithm formal point plan guard")

    cli_output = tmp_path / "cli_guard"
    rc = cli_main(
        [
            "--profile-path",
            str(PROFILE_PATH),
            "--output-dir",
            str(cli_output),
            "--fail-on-blocker",
        ]
    )
    assert rc == 0
    assert (cli_output / "v1_5_algorithm_formal_point_plan_guard_manifest.json").exists()
