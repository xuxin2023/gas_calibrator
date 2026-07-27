import copy
import json
from pathlib import Path

import pytest

from gas_calibrator.tools.export_v1_5_component_qc_reference_evaluator import main
from gas_calibrator.validation.v1_5_component_qc_reference_evaluator import (
    evaluate_v1_5_component_qc_reference_fixture,
    validate_synthetic_component_qc_fixture,
    write_v1_5_component_qc_reference_evaluation,
)
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint


CONTRACT_PATH = Path("configs/v1_5_component_qc_generator_contract.json")
FIXTURE_PATH = Path("tests/fixtures/v1_5_component_qc_reference_mixed_co2.json")


def _contract() -> dict:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def _fixture(component: str = "co2", span: float = 0.0004) -> dict:
    suffix = "co2_ratio_f" if component == "co2" else "h2o_ratio_f"
    return {
        "schema": "v1_5_component_qc_synthetic_fixture_v1",
        "synthetic_fixture": True,
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "component": component,
        "point_id": f"synthetic_{component}",
        "required_sample_count": 10,
        "analyzers": [{"label": "GA01", "prefix": "ga01"}],
        "temporal_window_complete": True,
        "cadence_warning": False,
        "sample_alignment_ok": True,
        "point_flags": {},
        "sample_rows": [
            {
                "timestamp_s": float(index),
                "ga01_frame_usable": True,
                f"ga01_{suffix}": 1.0 + span * index / 9,
            }
            for index in range(10)
        ],
    }


def _row(model: dict, prefix: str = "ga01") -> dict:
    return next(item for item in model["analyzers"] if item["prefix"] == prefix)


def test_mixed_fixture_keeps_analyzer_fit_eligibility_independent() -> None:
    fixture = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    model = evaluate_v1_5_component_qc_reference_fixture(fixture, _contract())
    ga01 = _row(model, "ga01")
    ga02 = _row(model, "ga02")
    assert ga01["grade"] == "A_calibration_eligible"
    assert ga01["sample_can_enter_calibration_fit"] is True
    assert ga02["grade"] == "C_reject"
    assert ga02["sample_can_enter_calibration_fit"] is False
    assert model["point_summary"]["worst_grade"] == "C_reject"
    assert model["point_summary"]["informational_only"] is True
    assert model["point_summary"]["one_analyzer_failure_blocks_other_analyzers"] is False
    assert model["sample_alignment_ok"] is False
    assert ga01["timestamps_strictly_increasing"] is True
    assert ga01["actual_window_duration_s"] == pytest.approx(9.0)
    assert ga01["minimum_window_duration_s"] == pytest.approx(8.1)
    assert ga01["cadence_warning"] is False
    assert ga01["evidence_identity_status"] == "pass"
    assert ga01["evidence_identity_mode"] == "synthetic_reference"
    assert ga01["evidence_bundle_manifest_verified"] is False
    assert len(ga01["evidence_bundle_sha256"]) == 64
    assert ga01["evidence_bundle_member_count"] == 4
    assert ga01["reference_source_status"] == "synthetic_not_formal_reference"
    assert ga01["reference_source_record_present"] is False
    assert ga01["reference_source_record_valid"] is False
    assert len(ga01["source_reference_source_sha256"]) == 64


@pytest.mark.parametrize(
    ("span", "grade"),
    [
        (0.0005, "A_calibration_eligible"),
        (0.001, "B_diagnostic_model_only"),
        (0.00101, "C_reject"),
    ],
)
def test_co2_span_boundaries_are_inclusive(span: float, grade: str) -> None:
    assert _row(evaluate_v1_5_component_qc_reference_fixture(_fixture("co2", span), _contract()))[
        "grade"
    ] == grade


@pytest.mark.parametrize(
    ("span", "grade"),
    [(0.001, "A_calibration_eligible"), (0.003, "B_diagnostic_model_only")],
)
def test_h2o_span_above_a_is_diagnostic_not_c(span: float, grade: str) -> None:
    row = _row(evaluate_v1_5_component_qc_reference_fixture(_fixture("h2o", span), _contract()))
    assert row["grade"] == grade
    assert row["sample_can_enter_diagnostic_model"] is True


def test_usable_frame_count_caps_at_b_then_rejects_below_floor() -> None:
    nine = _fixture()
    nine["sample_rows"][-1]["ga01_frame_usable"] = False
    eight = copy.deepcopy(nine)
    eight["sample_rows"][-2]["ga01_frame_usable"] = False
    nine_row = _row(evaluate_v1_5_component_qc_reference_fixture(nine, _contract()))
    eight_row = _row(evaluate_v1_5_component_qc_reference_fixture(eight, _contract()))
    assert nine_row["usable_ratio_count"] == 9
    assert nine_row["grade"] == "B_diagnostic_model_only"
    assert eight_row["usable_ratio_count"] == 8
    assert eight_row["grade"] == "C_reject"


def test_raw_usable_outlier_is_not_hidden_by_summary_values() -> None:
    fixture = _fixture()
    fixture["summary_outlier_filtered_ratio_span"] = 0.0001
    fixture["sample_rows"][-1]["ga01_co2_ratio_f"] = 1.002
    row = _row(evaluate_v1_5_component_qc_reference_fixture(fixture, _contract()))
    assert row["grade"] == "C_reject"
    assert row["ratio_span"] == pytest.approx(0.002)


def test_alignment_false_alone_does_not_reject_or_cap_grade() -> None:
    fixture = _fixture()
    fixture["sample_alignment_ok"] = False
    row = _row(evaluate_v1_5_component_qc_reference_fixture(fixture, _contract()))
    assert row["grade"] == "A_calibration_eligible"


def test_point_wide_physical_blocker_rejects_every_analyzer() -> None:
    fixture = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    fixture["point_flags"]["route_not_open_until_sample_end"] = True
    model = evaluate_v1_5_component_qc_reference_fixture(fixture, _contract())
    assert all(row["grade"] == "C_reject" for row in model["analyzers"])
    assert model["point_wide_hard_blockers"] == ["route_not_open_until_sample_end"]


def test_cadence_warning_caps_one_analyzer_but_incomplete_window_rejects() -> None:
    fixture = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    fixture["analyzer_evidence"] = {
        "ga01": {"cadence_warning": True},
        "ga02": {"temporal_window_complete": False},
    }
    model = evaluate_v1_5_component_qc_reference_fixture(fixture, _contract())
    assert _row(model, "ga01")["grade"] == "B_diagnostic_model_only"
    assert _row(model, "ga02")["grade"] == "C_reject"


@pytest.mark.parametrize(
    ("timestamps", "reason"),
    [
        ([index * 0.1 for index in range(10)], "sample_window_duration_below_minimum"),
        ([0, 1, 2, 3, 4, 5, 6, 6, 8, 9], "timestamps_not_strictly_increasing"),
        ([0, 1, 2, 3, 4, "bad", 6, 7, 8, 9], "timestamp_missing_or_unparseable"),
    ],
)
def test_derived_temporal_hard_failures_reject_without_manual_flags(
    timestamps, reason
) -> None:
    fixture = _fixture()
    for row, timestamp in zip(fixture["sample_rows"], timestamps):
        row["timestamp_s"] = timestamp

    evaluated = _row(
        evaluate_v1_5_component_qc_reference_fixture(fixture, _contract())
    )

    assert evaluated["temporal_window_complete"] is False
    assert evaluated["grade"] == "C_reject"
    assert f"temporal:{reason}" in evaluated["reason"]


def test_timestamp_row_count_below_required_rejects_instead_of_degrading_to_b() -> None:
    fixture = _fixture()
    fixture["sample_rows"].pop()

    evaluated = _row(
        evaluate_v1_5_component_qc_reference_fixture(fixture, _contract())
    )

    assert evaluated["timestamp_count"] == 9
    assert evaluated["required_timestamp_count"] == 10
    assert evaluated["grade"] == "C_reject"
    assert "temporal:timestamp_count_below_required:9<10" in evaluated["reason"]


def test_derived_cadence_warning_caps_grade_when_window_duration_is_complete() -> None:
    fixture = _fixture()
    timestamps = [0, 1, 2, 3, 4, 5, 6, 7, 8, 10.1]
    for row, timestamp in zip(fixture["sample_rows"], timestamps):
        row["timestamp_s"] = timestamp

    evaluated = _row(
        evaluate_v1_5_component_qc_reference_fixture(fixture, _contract())
    )

    assert evaluated["temporal_window_complete"] is True
    assert evaluated["cadence_warning"] is True
    assert evaluated["maximum_observed_interval_s"] == pytest.approx(2.1)
    assert evaluated["grade"] == "B_diagnostic_model_only"
    assert "cadence_warning_grade_capped_at_b" in evaluated["reason"]


def test_minimum_window_duration_boundary_is_inclusive() -> None:
    fixture = _fixture()
    for index, row in enumerate(fixture["sample_rows"]):
        row["timestamp_s"] = index * 0.9

    evaluated = _row(
        evaluate_v1_5_component_qc_reference_fixture(fixture, _contract())
    )

    assert evaluated["actual_window_duration_s"] == pytest.approx(8.1)
    assert evaluated["minimum_window_duration_s"] == pytest.approx(8.1)
    assert evaluated["temporal_window_complete"] is True
    assert evaluated["cadence_warning"] is False
    assert evaluated["grade"] == "A_calibration_eligible"


def test_missing_ratio_in_usable_frame_is_c() -> None:
    fixture = _fixture()
    del fixture["sample_rows"][0]["ga01_co2_ratio_f"]
    row = _row(evaluate_v1_5_component_qc_reference_fixture(fixture, _contract()))
    assert row["grade"] == "C_reject"
    assert "missing_or_nonfinite_ratio" in row["reason"]


def test_non_synthetic_and_historical_path_inputs_are_blocked() -> None:
    fixture = _fixture()
    fixture["synthetic_fixture"] = False
    fixture["source_samples_path"] = "D:/historical/point/samples.csv"
    reasons = validate_synthetic_component_qc_fixture(fixture)
    assert "synthetic_fixture_flag_required" in reasons
    assert "historical_or_device_field_forbidden:source_samples_path" in reasons
    with pytest.raises(ValueError):
        evaluate_v1_5_component_qc_reference_fixture(fixture, _contract())


def test_malformed_sample_row_is_blocked_before_evaluation() -> None:
    fixture = _fixture()
    fixture["sample_rows"][2] = "not-a-row"
    assert "sample_row_must_be_object" in validate_synthetic_component_qc_fixture(fixture)
    with pytest.raises(ValueError, match="sample_row_must_be_object"):
        evaluate_v1_5_component_qc_reference_fixture(fixture, _contract())


@pytest.mark.parametrize("invalid_count", [True, 9.5, 0, -1, "10"])
def test_required_sample_count_must_be_a_positive_integer(invalid_count: object) -> None:
    fixture = _fixture()
    fixture["required_sample_count"] = invalid_count
    with pytest.raises(ValueError, match="required_sample_count_must_be_positive_integer"):
        evaluate_v1_5_component_qc_reference_fixture(fixture, _contract())


def test_reference_output_is_idempotent_and_keeps_all_production_locks() -> None:
    fixture = _fixture()
    first = evaluate_v1_5_component_qc_reference_fixture(fixture, _contract())
    second = evaluate_v1_5_component_qc_reference_fixture(fixture, _contract())
    assert first == second
    assert first["fixture_sha256"] == second["fixture_sha256"]
    assert first["analyzers"] == second["analyzers"]
    assert first["locks"] == second["locks"]
    assert first["locks"]["reference_evaluator_available"] is True
    assert first["locks"]["production_component_qc_generator_available"] is False
    assert first["locks"]["historical_component_qc_write_allowed"] is False
    assert first["locks"]["opens_com_ports"] is False
    assert first["not_real_acceptance_evidence"] is True


def test_cli_writes_only_explicit_review_artifacts_and_is_offline(tmp_path: Path) -> None:
    model = evaluate_v1_5_component_qc_reference_fixture(
        json.loads(FIXTURE_PATH.read_text(encoding="utf-8")), _contract()
    )
    review_suffix = Path("docs/v1_5_flow_contract/component_qc_reference_evaluator")
    direct_output = tmp_path / "direct" / review_suffix
    cli_output = tmp_path / "cli" / review_suffix
    direct = write_v1_5_component_qc_reference_evaluation(model, direct_output)
    rc = main(
        [
            "--fixture-json-path",
            str(FIXTURE_PATH),
            "--contract-json-path",
            str(CONTRACT_PATH),
            "--output-dir",
            str(cli_output),
        ]
    )
    entry = classify_v1_5_entrypoint(
        Path("src/gas_calibrator/tools/export_v1_5_component_qc_reference_evaluator.py"),
        root=Path.cwd(),
    )
    assert rc == 0
    assert set(direct) == {"json", "analyzer_csv", "markdown"}
    assert entry.category == "formal_review_evidence"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False


def test_cli_rejects_real_fixture_without_writing_artifacts(tmp_path: Path) -> None:
    fixture = _fixture()
    fixture["evidence_source"] = "historical"
    fixture_path = tmp_path / "real.json"
    fixture_path.write_text(json.dumps(fixture), encoding="utf-8")
    output_dir = tmp_path / "blocked"
    rc = main(
        [
            "--fixture-json-path",
            str(fixture_path),
            "--contract-json-path",
            str(CONTRACT_PATH),
            "--output-dir",
            str(output_dir),
        ]
    )
    assert rc == 2
    assert not output_dir.exists()


def test_cli_rejects_arbitrary_or_historical_output_directory(tmp_path: Path) -> None:
    output_dir = tmp_path / "historical_point"
    rc = main(
        [
            "--fixture-json-path",
            str(FIXTURE_PATH),
            "--contract-json-path",
            str(CONTRACT_PATH),
            "--output-dir",
            str(output_dir),
        ]
    )
    assert rc == 2
    assert not output_dir.exists()
