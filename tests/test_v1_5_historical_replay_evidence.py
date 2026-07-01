import csv
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_historical_replay_evidence import main as cli_main
from gas_calibrator.validation.v1_5_historical_replay_evidence import (
    build_v1_5_historical_replay_evidence,
    write_v1_5_historical_replay_evidence,
)


ROOT = Path(__file__).resolve().parents[1]
PROFILE_PATH = ROOT / "configs" / "v1_5_algorithm_route_profiles.json"


def _load_profile() -> dict:
    return json.loads(PROFILE_PATH.read_text(encoding="utf-8"))


def _profile(profile_id: str) -> dict:
    return next(profile for profile in _load_profile()["profiles"] if profile["profile_id"] == profile_id)


def _temp_token(value: str | int | float) -> str:
    number = int(float(value))
    return f"Tm{abs(number)}" if number < 0 else f"T{number}"


def _write_formal_quality(path: Path, *, include_reject: bool = False) -> None:
    rows = [
        {
            "label": "ga01",
            "prefix": "ga01",
            "grade": "A_calibration_eligible",
            "ratio_key": "ga01_ratio_f",
            "reason": "",
            "sample_can_enter_calibration_fit": "True",
            "sample_can_enter_diagnostic_model": "True",
        }
    ]
    if include_reject:
        rows.append(
            {
                "label": "ga02",
                "prefix": "ga02",
                "grade": "C_reject",
                "ratio_key": "ga02_ratio_f",
                "reason": "ratio_span=0.0012>tol=0.0010",
                "sample_can_enter_calibration_fit": "False",
                "sample_can_enter_diagnostic_model": "False",
            }
        )
    with (path / "formal_open_flow_data_quality_by_analyzer.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _write_frame_quality(path: Path) -> None:
    rows = [
        {
            "mode": "current",
            "Analyzer": "GA01",
            "AnalyzerId": "001",
            "PointRow": "1",
            "PointPhase": "co2",
            "PointTag": "open_flow",
            "TotalFrames": "10",
            "ValidFrames": "10",
            "ValidRatio": "1.0",
            "UnusableReasonTopN": "可用=10",
        }
    ]
    with (path / "frame_quality_summary.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _write_conclusion(path: Path) -> None:
    with (path / "conclusion_summary.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["risk_level", "analyzer_count", "advice"])
        writer.writeheader()
        writer.writerow({"risk_level": "ok", "analyzer_count": "1", "advice": "fixture"})


def _create_route_fixture(
    root: Path,
    *,
    profile_id: str = "legacy_ratio_production",
    route_kind: str,
    skip_last: bool = False,
    missing_quality_for_first: bool = False,
    frame_quality_only: bool = False,
) -> Path:
    profile = _profile(profile_id)
    if route_kind == "co2":
        plan = profile["co2_route"]["temperature_plan"]
    else:
        plan = profile["h2o_route"].get("temperature_plan") or profile["h2o_route"]["wet_temperature_plan"]
    root.mkdir(parents=True, exist_ok=True)
    point_no = 1
    point_names: list[str] = []
    for temp, values in plan.items():
        for value in values:
            if route_kind == "co2":
                point_names.append(f"p{point_no:03d}_{_temp_token(temp)}_{int(value)}ppm_fit")
            else:
                token = str(value).replace("HGEN", "HG")
                point_names.append(f"p{point_no:03d}_{_temp_token(temp)}_{token}_h2o")
            point_no += 1
    if skip_last:
        point_names = point_names[:-1]
    for index, name in enumerate(point_names):
        point = root / name
        point.mkdir(parents=True, exist_ok=True)
        _write_conclusion(point)
        if missing_quality_for_first and index == 0:
            continue
        if frame_quality_only:
            _write_frame_quality(point)
        else:
            _write_formal_quality(point, include_reject=index == 0)
    return root


def _roots(co2_root: Path, h2o_root: Path) -> list[dict]:
    return [
        {
            "family_id": "mature_0620_legacy_ratio",
            "route_kind": "co2",
            "root_path": str(co2_root),
            "algorithm_profile_id": "legacy_ratio_production",
            "label": "fixture_co2",
        },
        {
            "family_id": "mature_0620_legacy_ratio",
            "route_kind": "h2o",
            "root_path": str(h2o_root),
            "algorithm_profile_id": "legacy_ratio_production",
            "label": "fixture_h2o",
        },
    ]


def _check_by_id(model: dict) -> dict[str, dict]:
    return {row["check_id"]: row for row in model["checks"]}


def test_historical_replay_evidence_binds_full_legacy_routes(tmp_path: Path) -> None:
    co2 = _create_route_fixture(tmp_path / "co2", route_kind="co2")
    h2o = _create_route_fixture(tmp_path / "h2o", route_kind="h2o")

    model = build_v1_5_historical_replay_evidence(profile_path=PROFILE_PATH, evidence_roots=_roots(co2, h2o))
    checks = _check_by_id(model)

    assert model["manifest"]["status"] == "pass"
    assert model["manifest"]["blocker_count"] == 0
    assert model["manifest"]["opens_com_ports"] is False
    assert model["manifest"]["writes_coefficients"] is False
    assert model["manifest"]["formal_release_allowed"] is False
    assert model["manifest"]["database_import_allowed"] is False
    assert {summary["route_kind"]: summary["observed_point_count"] for summary in model["route_summaries"]} == {
        "co2": 45,
        "h2o": 13,
    }
    assert checks["point_sequence_matches_profile_or_requires_review"]["status"] == "pass"
    assert checks["quality_evidence_present"]["status"] == "pass"
    assert checks["rejected_rows_preserved"]["status"] == "pass"
    assert any(point["rejected_rows"] == 1 for point in model["points"])


def test_historical_replay_evidence_marks_missing_points_review_required(tmp_path: Path) -> None:
    co2 = _create_route_fixture(tmp_path / "co2", route_kind="co2", skip_last=True)
    h2o = _create_route_fixture(tmp_path / "h2o", route_kind="h2o")

    model = build_v1_5_historical_replay_evidence(profile_path=PROFILE_PATH, evidence_roots=_roots(co2, h2o))
    checks = _check_by_id(model)
    co2_summary = next(row for row in model["route_summaries"] if row["route_kind"] == "co2")

    assert model["manifest"]["status"] == "review_required"
    assert model["manifest"]["blocker_count"] == 0
    assert checks["point_sequence_matches_profile_or_requires_review"]["status"] == "review_required"
    assert co2_summary["observed_point_count"] == 44
    assert co2_summary["missing_expected_points"]


def test_historical_replay_evidence_blocks_missing_quality(tmp_path: Path) -> None:
    co2 = _create_route_fixture(tmp_path / "co2", route_kind="co2", missing_quality_for_first=True)
    h2o = _create_route_fixture(tmp_path / "h2o", route_kind="h2o")

    model = build_v1_5_historical_replay_evidence(profile_path=PROFILE_PATH, evidence_roots=_roots(co2, h2o))
    checks = _check_by_id(model)

    assert model["manifest"]["status"] == "blocked"
    assert checks["quality_evidence_present"]["status"] == "blocker"


def test_historical_replay_evidence_accepts_frame_quality_only_points(tmp_path: Path) -> None:
    co2 = _create_route_fixture(tmp_path / "co2", route_kind="co2", frame_quality_only=True)
    h2o = _create_route_fixture(tmp_path / "h2o", route_kind="h2o", frame_quality_only=True)

    model = build_v1_5_historical_replay_evidence(profile_path=PROFILE_PATH, evidence_roots=_roots(co2, h2o))

    assert model["manifest"]["status"] == "pass"
    assert {point["quality_source"] for point in model["points"]} == {"frame_quality_summary"}


def test_historical_replay_evidence_binds_new_algorithm_absorption_profile(tmp_path: Path) -> None:
    co2 = _create_route_fixture(tmp_path / "co2_new", profile_id="absorption_ratio_shadow", route_kind="co2")
    roots = [
        {
            "family_id": "new_algorithm_shadow_run",
            "route_kind": "co2",
            "root_path": str(co2),
            "algorithm_profile_id": "absorption_ratio_shadow",
            "label": "new_algorithm_co2_fixture",
        }
    ]

    model = build_v1_5_historical_replay_evidence(profile_path=PROFILE_PATH, evidence_roots=roots)
    fit_check = _check_by_id(model)["fit_input_profile_bound"]

    assert model["manifest"]["status"] == "pass"
    assert "A=-ln(R/R0(T))/(P_kPa/100)" in fit_check["observed"]
    assert model["route_summaries"][0]["observed_point_count"] == 45


def test_historical_replay_evidence_writer_and_cli(tmp_path: Path) -> None:
    co2 = _create_route_fixture(tmp_path / "co2", route_kind="co2")
    h2o = _create_route_fixture(tmp_path / "h2o", route_kind="h2o")
    output = tmp_path / "out"

    outputs = write_v1_5_historical_replay_evidence(
        profile_path=PROFILE_PATH,
        evidence_roots=_roots(co2, h2o),
        output_dir=output,
    )
    manifest = json.loads(Path(outputs["manifest"]).read_text(encoding="utf-8"))
    assert manifest["manifest"]["status"] == "pass"
    assert (output / "v1_5_historical_replay_points.csv").exists()
    assert Path(outputs["markdown"]).read_text(encoding="utf-8").startswith("# V1.5 Historical Replay Evidence")

    cli_output = tmp_path / "cli"
    rc = cli_main(
        [
            "--profile-path",
            str(PROFILE_PATH),
            "--output-dir",
            str(cli_output),
            "--evidence-root",
            f"mature_0620_legacy_ratio:co2={co2}",
            "--evidence-root",
            f"mature_0620_legacy_ratio:h2o={h2o}",
            "--fail-on-blocker",
        ]
    )

    assert rc == 0
    assert (cli_output / "v1_5_historical_replay_evidence.json").exists()
