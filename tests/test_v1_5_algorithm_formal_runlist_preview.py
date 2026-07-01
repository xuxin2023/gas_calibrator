import csv
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_algorithm_formal_runlist_preview import main as cli_main
from gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue import _load_queue_rows as load_co2_queue
from gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_queue import _load_queue_rows as load_h2o_queue
from gas_calibrator.validation.v1_5_algorithm_route_profiles import (
    build_v1_5_algorithm_formal_runlist_preview,
    write_v1_5_algorithm_formal_runlist_preview,
)


ROOT = Path(__file__).resolve().parents[1]
PROFILE_PATH = ROOT / "configs" / "v1_5_algorithm_route_profiles.json"


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _co2_segment(rows: list[dict], temperature_c: int) -> list[int]:
    return [
        int(float(row["source_nominal_ppm"]))
        for row in rows
        if int(float(row["temp_c"])) == temperature_c
    ]


def _h2o_segment(rows: list[dict], *, temperature_c: int, hgen_temp_c: int) -> list[int]:
    return [
        int(float(row["hgen_rh_pct"]))
        for row in rows
        if int(float(row["temp_c"])) == temperature_c
        and int(float(row["hgen_temp_c"])) == hgen_temp_c
    ]


def test_algorithm_formal_runlist_preview_locks_new_algorithm_counts_and_boundaries() -> None:
    tables = build_v1_5_algorithm_formal_runlist_preview(PROFILE_PATH)
    manifest = tables["manifest"]

    assert manifest["status"] == "pass"
    assert manifest["blocker_count"] == 0
    assert manifest["no_write"] is True
    assert manifest["opens_com_ports"] is False
    assert manifest["connects_postgresql"] is False
    assert manifest["controls_water_or_gas_routes"] is False
    assert manifest["writes_coefficients"] is False
    assert manifest["runner_integration_status"] == "preview_only_not_runner_wired"
    assert manifest["legacy_co2_formal_point_count"] == 45
    assert manifest["legacy_h2o_formal_point_count"] == 13
    assert manifest["co2_runlist_count"] == 47
    assert manifest["h2o_runlist_count"] == 14

    assert _co2_segment(tables["co2_runlist"], -20) == [0, 400, 600, 1000]
    assert _co2_segment(tables["co2_runlist"], -10) == [0, 400, 600, 1000]
    assert _h2o_segment(tables["h2o_runlist"], temperature_c=40, hgen_temp_c=30) == [30, 50, 70]
    co2_groups = {
        (int(float(row["temp_c"])), int(float(row["source_nominal_ppm"]))): row["co2_group"]
        for row in tables["co2_runlist"]
    }
    assert co2_groups[(-20, 600)] == "A"
    assert co2_groups[(10, 100)] == "B"
    assert co2_groups[(10, 200)] == "A"
    assert co2_groups[(10, 900)] == "B"
    assert co2_groups[(10, 1000)] == "A"


def test_algorithm_formal_runlist_preview_marks_supplements_as_formal_points() -> None:
    tables = build_v1_5_algorithm_formal_runlist_preview(PROFILE_PATH)
    supplemental = [
        row
        for row in [*tables["co2_runlist"], *tables["h2o_runlist"]]
        if row["point_role"] == "new_algorithm_required_supplemental_formal_point"
    ]

    assert {
        (row["component"], row["point_id"], row["source_point_key"], row["historical_missing_point_semantics"])
        for row in supplemental
    } == {
        ("co2", "co2_T-20_600ppm_ambient", "-20/600", "formal_required_point_not_historical_resampling"),
        ("co2", "co2_T-10_600ppm_ambient", "-10/600", "formal_required_point_not_historical_resampling"),
        ("h2o", "h2o_T40_HGEN30C_30RH_ambient", "40/30/30", "formal_required_point_not_historical_resampling"),
    }
    assert {row["runner_integration_status"] for row in supplemental} == {
        "preview_only_not_runner_wired"
    }
    assert {row["do_not_modify_mature_runner"] for row in supplemental} == {True}


def test_algorithm_formal_runlist_preview_writes_queue_compatible_csvs(tmp_path: Path) -> None:
    output = tmp_path / "runlist"
    outputs = write_v1_5_algorithm_formal_runlist_preview(PROFILE_PATH, output)

    manifest = json.loads(Path(outputs["manifest"]).read_text(encoding="utf-8-sig"))
    assert manifest["status"] == "pass"
    co2_rows = _read_csv(Path(outputs["co2_runlist"]))
    h2o_rows = _read_csv(Path(outputs["h2o_runlist"]))
    assert len(co2_rows) == 47
    assert len(h2o_rows) == 14
    assert (output / "V1_5_ALGORITHM_FORMAL_RUNLIST_PREVIEW.md").exists()

    loaded_co2 = load_co2_queue(outputs["co2_runlist"])
    loaded_h2o = load_h2o_queue(outputs["h2o_runlist"])
    assert len(loaded_co2) == 47
    assert len(loaded_h2o) == 14
    assert _co2_segment(loaded_co2, -20) == [0, 400, 600, 1000]
    assert _h2o_segment(loaded_h2o, temperature_c=40, hgen_temp_c=30) == [30, 50, 70]

    cli_output = tmp_path / "cli_runlist"
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
    assert (cli_output / "v1_5_new_algorithm_formal_co2_runlist_preview.csv").exists()
    assert (cli_output / "v1_5_new_algorithm_formal_h2o_runlist_preview.csv").exists()
