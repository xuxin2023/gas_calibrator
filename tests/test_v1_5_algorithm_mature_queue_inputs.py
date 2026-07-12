import csv
import json
from pathlib import Path

import pytest

from gas_calibrator.tools.export_v1_5_algorithm_mature_queue_inputs import main
from gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue import (
    _load_queue_rows as load_co2_rows,
)
from gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_queue import (
    _load_queue_rows as load_h2o_rows,
)
from gas_calibrator.validation.v1_5_algorithm_mature_queue_inputs import (
    CO2_QUEUE_RUNNER,
    H2O_QUEUE_RUNNER,
    build_v1_5_algorithm_mature_queue_inputs,
    write_v1_5_algorithm_mature_queue_inputs,
)
from gas_calibrator.validation.v1_5_entrypoint_inventory import (
    classify_v1_5_entrypoint,
)

ROOT = Path(__file__).resolve().parents[1]
PROFILE = ROOT / "configs" / "v1_5_algorithm_route_profiles.json"


@pytest.mark.parametrize(
    ("profile_id", "co2_count", "h2o_count"),
    [
        ("legacy_ratio_production", 45, 13),
        ("absorption_ratio_shadow", 47, 14),
    ],
)
def test_materializer_locks_profile_counts_and_mature_runners(
    profile_id: str, co2_count: int, h2o_count: int
) -> None:
    model = build_v1_5_algorithm_mature_queue_inputs(
        profile_path=PROFILE,
        profile_id=profile_id,
    )
    assert model["co2_point_count"] == co2_count
    assert model["h2o_point_count"] == h2o_count
    assert model["co2_queue_runner"] == CO2_QUEUE_RUNNER
    assert model["h2o_queue_runner"] == H2O_QUEUE_RUNNER
    assert model["profile_declared_queue_source_is_not_consumed"] is True
    assert model["mature_point_execution_is_not_copied_or_modified"] is True
    assert model["opens_com_ports"] is False
    assert model["controls_water_or_gas_routes"] is False
    assert model["writes_coefficients"] is False
    assert "_handoff" not in json.dumps(model, ensure_ascii=False).lower()
    assert "0624" not in json.dumps(model, ensure_ascii=False).lower()


def test_new_algorithm_supplements_are_inside_mature_queue_segments() -> None:
    model = build_v1_5_algorithm_mature_queue_inputs(
        profile_path=PROFILE,
        profile_id="absorption_ratio_shadow",
    )
    co2 = [(row["temp_c"], row["source_nominal_ppm"]) for row in model["co2_rows"]]
    h2o = [
        (row["temp_c"], row["hgen_temp_c"], row["hgen_rh_pct"])
        for row in model["h2o_rows"]
    ]
    assert co2.index((-20.0, 600.0)) == co2.index((-20.0, 400.0)) + 1
    assert co2.index((-10.0, 600.0)) == co2.index((-10.0, 400.0)) + 1
    assert h2o.index((40.0, 30.0, 30.0)) < h2o.index((40.0, 30.0, 50.0))


def test_written_csvs_are_consumed_by_unchanged_mature_queue_loaders(
    tmp_path: Path,
) -> None:
    model = write_v1_5_algorithm_mature_queue_inputs(
        profile_path=PROFILE,
        profile_id="absorption_ratio_shadow",
        output_dir=tmp_path,
    )
    assert len(load_co2_rows(model["co2_queue_csv"])) == 47
    assert len(load_h2o_rows(model["h2o_queue_csv"])) == 14
    assert Path(model["manifest_json"]).is_file()
    with Path(model["co2_queue_csv"]).open(
        "r", encoding="utf-8-sig", newline=""
    ) as handle:
        rows = list(csv.DictReader(handle))
    assert {row["runner_integration_status"] for row in rows} == {
        "profile_generated_mature_queue_input"
    }


def test_materializer_rejects_unknown_profile(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Unsupported V1.5 algorithm profile"):
        write_v1_5_algorithm_mature_queue_inputs(
            profile_path=PROFILE,
            profile_id="migrated_0624",
            output_dir=tmp_path,
        )


@pytest.mark.parametrize("tamper", ("point", "runner"))
def test_materializer_rejects_same_count_point_or_runner_substitution(
    tmp_path: Path, tamper: str
) -> None:
    payload = json.loads(PROFILE.read_text(encoding="utf-8-sig"))
    profile = next(
        row for row in payload["profiles"] if row["profile_id"] == "absorption_ratio_shadow"
    )
    if tamper == "point":
        profile["co2_route"]["temperature_plan"]["-20"] = [0, 500, 1000]
    else:
        profile["co2_route"]["runner"] = "migration.run_v1_5_co2"
    altered = tmp_path / "altered_profiles.json"
    altered.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError):
        write_v1_5_algorithm_mature_queue_inputs(
            profile_path=altered,
            profile_id="absorption_ratio_shadow",
            output_dir=tmp_path / "queues",
        )


def test_cli_is_offline_formal_support(tmp_path: Path) -> None:
    output = tmp_path / "queues"
    assert (
        main(
            [
                "--profile-path",
                str(PROFILE),
                "--profile-id",
                "legacy_ratio_production",
                "--output-dir",
                str(output),
            ]
        )
        == 0
    )
    entry = classify_v1_5_entrypoint(
        ROOT
        / "src/gas_calibrator/tools/export_v1_5_algorithm_mature_queue_inputs.py",
        root=ROOT,
    )
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
