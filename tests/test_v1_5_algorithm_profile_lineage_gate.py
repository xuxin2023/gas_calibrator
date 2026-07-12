import json
from pathlib import Path

import pytest

from gas_calibrator.tools.export_v1_5_algorithm_profile_lineage_gate import main
from gas_calibrator.validation.v1_5_algorithm_mature_queue_inputs import (
    write_v1_5_algorithm_mature_queue_inputs,
)
from gas_calibrator.validation.v1_5_algorithm_profile_lineage_gate import (
    build_v1_5_algorithm_profile_lineage_gate,
)
from gas_calibrator.validation.v1_5_entrypoint_inventory import (
    classify_v1_5_entrypoint,
)

ROOT = Path(__file__).resolve().parents[1]
PROFILE = ROOT / "configs" / "v1_5_algorithm_route_profiles.json"


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _fixture(tmp_path: Path, profile_id: str) -> dict:
    queue_model = write_v1_5_algorithm_mature_queue_inputs(
        profile_path=PROFILE,
        profile_id=profile_id,
        output_dir=tmp_path / "queues",
    )
    bootstrap = _write_json(
        tmp_path / "v1_5_new_run_bootstrap.json",
        {
            "algorithm_profile_id": profile_id,
            "algorithm_profile_snapshot_sha256": queue_model["profile_sha256"],
            "co2_queue_csv": queue_model["co2_queue_csv"],
            "co2_queue_sha256": queue_model["co2_queue_sha256"],
            "h2o_queue_csv": queue_model["h2o_queue_csv"],
            "h2o_queue_sha256": queue_model["h2o_queue_sha256"],
        },
    )
    co2_summary = _write_json(
        tmp_path / "co2_summary.json",
        {
            "queue_csv": queue_model["co2_queue_csv"],
            "selected_points": queue_model["co2_point_count"],
            "ok_points": queue_model["co2_point_count"],
            "failed_points": 0,
            "dry_run": False,
            "status": "ok",
        },
    )
    h2o_summary = _write_json(
        tmp_path / "h2o_summary.json",
        {
            "queue_csv": queue_model["h2o_queue_csv"],
            "selected_points": queue_model["h2o_point_count"],
            "ok_points": queue_model["h2o_point_count"],
            "failed_points": 0,
            "dry_run": False,
            "status": "ok",
        },
    )
    r0 = {}
    if profile_id == "absorption_ratio_shadow":
        for component, variable in (("co2", "R0_CO2(T)"), ("h2o", "R0_H2O(T)")):
            r0[component] = _write_json(
                tmp_path / f"{component}_r0.json",
                {
                    "profile_id": profile_id,
                    "profile_sha256": queue_model["profile_sha256"],
                    "component": component,
                    "status": "pass",
                    "model_variable": variable,
                    "opens_com_ports": False,
                    "writes_coefficients": False,
                    "connects_postgresql": False,
                },
            )
    return {
        "bootstrap_json": bootstrap,
        "queue_inputs_json": Path(queue_model["manifest_json"]),
        "co2_queue_summary_json": co2_summary,
        "h2o_queue_summary_json": h2o_summary,
        "co2_r0_model_json": r0.get("co2"),
        "h2o_r0_model_json": r0.get("h2o"),
    }


@pytest.mark.parametrize(
    ("profile_id", "co2_variable", "h2o_variable"),
    [
        (
            "legacy_ratio_production",
            "R_CO2_with_chamber_temperature_terms",
            "R_H2O_with_chamber_temperature_terms",
        ),
        (
            "absorption_ratio_shadow",
            "A_CO2=-ln(R_CO2/R0_CO2(T))/(P_kPa/100)",
            "A_H2O=-ln(R_H2O/R0_H2O(T))/(P_kPa/100)",
        ),
    ],
)
def test_lineage_gate_passes_only_the_profile_specific_fit_contract(
    tmp_path: Path, profile_id: str, co2_variable: str, h2o_variable: str
) -> None:
    model = build_v1_5_algorithm_profile_lineage_gate(**_fixture(tmp_path, profile_id))
    contract = model["fit_input_contract"]
    assert model["overall_status"] == "pass"
    assert model["fit_input_allowed"] is True
    assert contract["profile_id"] == profile_id
    assert contract["co2_fit_input"] == co2_variable
    assert contract["h2o_fit_input"] == h2o_variable
    assert contract["temperature_source"] == "per_analyzer_chamber_T1"
    assert contract["co2_zero_and_h2o_dry_anchor_are_separate"] is True
    assert model["opens_com_ports"] is False
    assert model["writes_coefficients"] is False


def test_absorption_profile_blocks_without_both_r0_models(tmp_path: Path) -> None:
    inputs = _fixture(tmp_path, "absorption_ratio_shadow")
    inputs["h2o_r0_model_json"] = None
    model = build_v1_5_algorithm_profile_lineage_gate(**inputs)
    reasons = {reason for row in model["checks"] for reason in row["reasons"]}
    assert model["overall_status"] == "blocked"
    assert model["fit_input_allowed"] is False
    assert "h2o_r0_model_missing" in reasons


@pytest.mark.parametrize("tamper", ("profile", "queue"))
def test_lineage_gate_blocks_profile_or_queue_substitution(
    tmp_path: Path, tamper: str
) -> None:
    inputs = _fixture(tmp_path, "legacy_ratio_production")
    if tamper == "profile":
        bootstrap = json.loads(Path(inputs["bootstrap_json"]).read_text(encoding="utf-8"))
        bootstrap["algorithm_profile_id"] = "absorption_ratio_shadow"
        _write_json(Path(inputs["bootstrap_json"]), bootstrap)
    else:
        queue_inputs = json.loads(Path(inputs["queue_inputs_json"]).read_text(encoding="utf-8"))
        Path(queue_inputs["co2_queue_csv"]).write_text("tampered\n", encoding="utf-8")
    model = build_v1_5_algorithm_profile_lineage_gate(**inputs)
    assert model["overall_status"] == "blocked"
    assert model["fit_input_allowed"] is False


def test_cli_fails_closed_and_is_offline_formal_support(tmp_path: Path) -> None:
    inputs = _fixture(tmp_path, "absorption_ratio_shadow")
    inputs["co2_r0_model_json"] = None
    args = []
    for key, value in inputs.items():
        if value:
            args.extend([f"--{key.replace('_', '-')}", str(value)])
    args.extend(["--output-dir", str(tmp_path / "out"), "--fail-on-blocker"])
    assert main(args) == 2
    entry = classify_v1_5_entrypoint(
        ROOT / "src/gas_calibrator/tools/export_v1_5_algorithm_profile_lineage_gate.py",
        root=ROOT,
    )
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
