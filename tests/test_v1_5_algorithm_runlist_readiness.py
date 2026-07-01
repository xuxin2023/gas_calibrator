import csv
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_algorithm_runlist_readiness import main as cli_main
from gas_calibrator.validation.v1_5_algorithm_route_profiles import (
    write_v1_5_algorithm_formal_runlist_preview,
)
from gas_calibrator.validation.v1_5_algorithm_runlist_readiness import (
    build_v1_5_algorithm_runlist_readiness,
    write_v1_5_algorithm_runlist_readiness_outputs,
)


ROOT = Path(__file__).resolve().parents[1]
PROFILE_PATH = ROOT / "configs" / "v1_5_algorithm_route_profiles.json"


def _preview_dir(tmp_path: Path) -> Path:
    output = tmp_path / "runlist_preview"
    write_v1_5_algorithm_formal_runlist_preview(PROFILE_PATH, output)
    return output


def _rewrite_without_point(path: Path, *, source_point_key: str) -> None:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
        fields = list(rows[0].keys())
    rows = [row for row in rows if row.get("source_point_key") != source_point_key]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _check(model: dict, name: str) -> dict:
    return {row["check"]: row for row in model["checks"]}[name]


def test_algorithm_runlist_readiness_passes_for_complete_preview(tmp_path: Path) -> None:
    preview = _preview_dir(tmp_path)

    model = build_v1_5_algorithm_runlist_readiness(runlist_dir=preview)

    assert model["schema"] == "v1_5_algorithm_runlist_readiness_v1"
    assert model["overall_status"] == "ready_for_new_algorithm_runner_integration_review"
    assert model["blocker_count"] == 0
    assert model["not_real_acceptance_evidence"] is True
    assert model["opens_com_ports"] is False
    assert model["connects_postgresql"] is False
    assert model["controls_water_or_gas_routes"] is False
    assert model["writes_coefficients"] is False
    assert model["runner_integration_status"] == "preview_only_not_runner_wired"
    assert model["legacy_co2_formal_point_count"] == 45
    assert model["legacy_h2o_formal_point_count"] == 13
    assert model["co2_runlist_count"] == 47
    assert model["h2o_runlist_count"] == 14
    assert _check(model, "new_algorithm_co2_runlist_47_point_gate")["status"] == "ready"
    assert _check(model, "new_algorithm_h2o_runlist_14_point_gate")["status"] == "ready"
    assert _check(model, "formal_supplemental_point_semantics_gate")["details"]["observed"] == [
        "-10/600",
        "-20/600",
        "40/30/30",
    ]


def test_algorithm_runlist_readiness_blocks_missing_co2_supplement(tmp_path: Path) -> None:
    preview = _preview_dir(tmp_path)
    _rewrite_without_point(preview / "v1_5_new_algorithm_formal_co2_runlist_preview.csv", source_point_key="-20/600")

    model = build_v1_5_algorithm_runlist_readiness(runlist_dir=preview)

    assert model["overall_status"] == "blocked"
    assert model["blocker_count"] >= 1
    co2_gate = _check(model, "new_algorithm_co2_runlist_47_point_gate")
    semantic_gate = _check(model, "formal_supplemental_point_semantics_gate")
    assert co2_gate["status"] == "blocker"
    assert "co2_runlist_count=46" in co2_gate["reasons"]
    assert "co2_minus20_segment_missing_600ppm_or_order" in co2_gate["reasons"]
    assert semantic_gate["status"] == "blocker"
    assert "missing_formal_supplements=-20/600" in semantic_gate["reasons"]


def test_algorithm_runlist_readiness_blocks_missing_h2o_supplement(tmp_path: Path) -> None:
    preview = _preview_dir(tmp_path)
    _rewrite_without_point(preview / "v1_5_new_algorithm_formal_h2o_runlist_preview.csv", source_point_key="40/30/30")

    model = build_v1_5_algorithm_runlist_readiness(runlist_dir=preview)

    assert model["overall_status"] == "blocked"
    h2o_gate = _check(model, "new_algorithm_h2o_runlist_14_point_gate")
    semantic_gate = _check(model, "formal_supplemental_point_semantics_gate")
    assert h2o_gate["status"] == "blocker"
    assert "h2o_runlist_count=13" in h2o_gate["reasons"]
    assert "h2o_40c_hgen30_segment_missing_30rh_or_order" in h2o_gate["reasons"]
    assert "h2o_40c_30rh_reference_bridge_status_missing" in h2o_gate["reasons"]
    assert semantic_gate["status"] == "blocker"
    assert "missing_formal_supplements=40/30/30" in semantic_gate["reasons"]


def test_algorithm_runlist_readiness_writer_and_cli(tmp_path: Path) -> None:
    preview = _preview_dir(tmp_path)
    model = build_v1_5_algorithm_runlist_readiness(runlist_dir=preview)

    outputs = write_v1_5_algorithm_runlist_readiness_outputs(model, tmp_path / "readiness")

    assert outputs["json"].exists()
    assert outputs["checks_csv"].exists()
    assert outputs["markdown"].exists()
    payload = json.loads(outputs["json"].read_text(encoding="utf-8-sig"))
    assert payload["overall_status"] == "ready_for_new_algorithm_runner_integration_review"
    assert "40/30/30" in outputs["markdown"].read_text(encoding="utf-8")

    cli_out = tmp_path / "cli_readiness"
    rc = cli_main(
        [
            "--runlist-dir",
            str(preview),
            "--output-dir",
            str(cli_out),
            "--fail-on-blocker",
        ]
    )
    assert rc == 0
    assert (cli_out / "v1_5_algorithm_runlist_readiness.json").exists()
