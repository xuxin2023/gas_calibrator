import csv
import hashlib
import json
from pathlib import Path

import pytest

from gas_calibrator.tools.export_v1_5_historical_fit_evidence_normalizer import main
from gas_calibrator.validation.v1_5_algorithm_mature_queue_inputs import (
    write_v1_5_algorithm_mature_queue_inputs,
)
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_historical_fit_evidence_normalizer import (
    build_v1_5_historical_fit_evidence_normalizer,
    write_v1_5_historical_fit_evidence_normalizer,
)
from gas_calibrator.validation.v1_5_historical_fit_profile_parity import (
    build_v1_5_historical_fit_profile_parity,
)

ROOT = Path(__file__).resolve().parents[1]
PROFILE = ROOT / "configs" / "v1_5_algorithm_route_profiles.json"


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _write_csv(path: Path, rows: list[dict]) -> Path:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _read_csv(path: str | Path) -> list[dict[str, str]]:
    with Path(path).open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _attestation_binding_fields(tmp_path: Path, label: str) -> dict[str, str]:
    summary = _write_json(tmp_path / f"{label}_queue_summary.json", {"status": "closed"})
    manifest = _write_csv(tmp_path / f"{label}_queue_manifest.csv", [{"status": "ok"}])
    inventory = _write_csv(
        tmp_path / f"{label}_evidence_inventory.csv",
        [{"role": "queue_summary", "path": str(summary), "sha256": _sha256(summary)}],
    )
    return {
        "binder_schema": "v1_5_historical_route_attestation_binder_v1",
        "queue_summary_path": str(summary),
        "queue_summary_sha256": _sha256(summary),
        "queue_manifest_path": str(manifest),
        "queue_manifest_sha256": _sha256(manifest),
        "evidence_inventory_path": str(inventory),
        "evidence_inventory_sha256": _sha256(inventory),
    }


def _fixture(tmp_path: Path, profile_id: str) -> dict:
    queue = write_v1_5_algorithm_mature_queue_inputs(
        profile_path=PROFILE,
        profile_id=profile_id,
        output_dir=tmp_path / "queues",
    )
    co2_queue = _read_csv(queue["co2_queue_csv"])
    h2o_queue = _read_csv(queue["h2o_queue_csv"])
    absorption = profile_id == "absorption_ratio_shadow"
    t1_by_component = {
        "co2": sorted({float(row["temp_c"]) + 0.25 for row in co2_queue}),
        "h2o": sorted(
            {float(row["temp_c"]) + 0.25 for row in h2o_queue}
            | {float(row["temp_c"]) + 0.25 for row in co2_queue if float(row["source_nominal_ppm"]) == 0.0}
        ),
    }
    r0_paths: dict[str, Path] = {}
    if absorption:
        for component, variable in (("co2", "R0_CO2(T)"), ("h2o", "R0_H2O(T)")):
            r0_paths[component] = _write_json(
                tmp_path / f"{component}_r0.json",
                {
                    "profile_id": profile_id,
                    "profile_sha256": queue["profile_sha256"],
                    "component": component,
                    "status": "pass",
                    "model_variable": variable,
                    "evaluated_points": [
                        {"temperature_c": temp, "r0_value": 1.2 + temp * 0.0001}
                        for temp in t1_by_component[component]
                    ],
                    "opens_com_ports": False,
                    "writes_coefficients": False,
                    "connects_postgresql": False,
                },
            )
    lineage = _write_json(
        tmp_path / "lineage.json",
        {
            "overall_status": "pass",
            "fit_input_allowed": True,
            "fit_input_contract": {
                "profile_id": profile_id,
                "profile_sha256": queue["profile_sha256"],
                "algorithm_mode": queue["algorithm_mode"],
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
                "queue_inputs_json": queue["manifest_json"],
                "co2_r0_model_json": str(r0_paths.get("co2", "")),
                "h2o_r0_model_json": str(r0_paths.get("h2o", "")),
            },
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
            "connects_postgresql": False,
        },
    )
    family_id = "legacy_0620" if not absorption else "new_0620"
    root = tmp_path / f"{family_id}_mature"
    points: list[dict] = []

    def add_point(route: str, index: int, queue_row: dict[str, str]) -> None:
        temp = float(queue_row["temp_c"])
        if route == "co2":
            ppm = float(queue_row["source_nominal_ppm"])
            point_id = f"p{index:03d}_T{temp:g}_{ppm:g}ppm"
            point = {
                "family_id": family_id,
                "route_kind": "co2",
                "point_id": point_id,
                "temp_c": temp,
                "co2_ppm": ppm,
                "hgen_c": None,
                "rh_pct": None,
            }
        else:
            hgen = float(queue_row["hgen_temp_c"])
            rh = float(queue_row["hgen_rh_pct"])
            point_id = f"p{index:03d}_T{temp:g}_HG{hgen:g}C_{rh:g}RH"
            point = {
                "family_id": family_id,
                "route_kind": "h2o",
                "point_id": point_id,
                "temp_c": temp,
                "co2_ppm": None,
                "hgen_c": hgen,
                "rh_pct": rh,
            }
        point_dir = root / point_id
        point["point_path"] = str(point_dir)
        t1 = temp + 0.25
        sample = {
            "ga01_id": "001",
            "ga01_frame_usable": "True",
            "ga01_co2_ratio_f": 1.1 - (float(point.get("co2_ppm") or 0.0) * 0.00001),
            "ga01_h2o_ratio_f": 0.8 - (float(point.get("rh_pct") or 0.0) * 0.0001),
            "ga01_chamber_temp_c": t1,
            "ga01_pressure_kpa": 101.2,
            "dewpoint_c": -35.0 if route == "co2" else 2.0,
            "co2_ppm": 9999.0,
            "h2o_mmol": 9999.0,
        }
        _write_csv(point_dir / "samples_machine_readable.csv", [sample, sample])
        quality_rows = [
            {
                "label": "ga01",
                "prefix": "ga01",
                "grade": "A_calibration_eligible",
                "ratio_key": f"ga01_{route}_ratio_f",
                "frame_count": "2",
                "usable_ratio_count": "2",
                "sample_can_enter_calibration_fit": "True",
                "reason": "",
            }
        ]
        if route == "co2" and float(point["co2_ppm"]) == 0.0:
            quality_rows.append(
                {
                    "label": "ga01",
                    "prefix": "ga01",
                    "grade": "A_calibration_eligible",
                    "ratio_key": "ga01_h2o_ratio_f",
                    "frame_count": "2",
                    "usable_ratio_count": "2",
                    "sample_can_enter_calibration_fit": "True",
                    "reason": "",
                }
            )
        _write_csv(point_dir / "formal_open_flow_data_quality_by_analyzer.csv", quality_rows)
        points.append(point)

    for index, row in enumerate(co2_queue, start=1):
        add_point("co2", index, row)
    for index, row in enumerate(h2o_queue, start=1):
        add_point("h2o", index, row)
    replay = _write_json(
        tmp_path / "replay.json",
        {
            "evidence_roots": [
                {
                    "family_id": family_id,
                    "route_kind": "mixed",
                    "root_path": str(root),
                    "algorithm_profile_id": profile_id,
                }
            ],
            "points": points,
        },
    )
    attestation = _write_json(
        tmp_path / "route_attestation.json",
        {
            "schema": "v1_5_historical_route_baseline_attestation_v1",
            "families": [
                {
                    "family_id": family_id,
                    "route_kind": "mixed",
                    "root_path": str(root),
                    "route_baseline": "0620",
                    "fitting_baseline": "0613",
                    "status": "reviewed",
                    "reviewer": "offline-test-reviewer",
                    "reviewed_at": "2026-07-13T00:00:00+08:00",
                    "not_0624_or_migration_source": True,
                    "mature_contract": "0613_fit_0620_0621_route",
                    **_attestation_binding_fields(tmp_path, family_id),
                }
            ],
        },
    )
    return {
        "lineage": lineage,
        "replay": replay,
        "attestation": attestation,
        "root": root,
        "queue": queue,
        "points": points,
    }


@pytest.mark.parametrize(
    ("profile_id", "fit_variable", "expected_co2", "expected_h2o"),
    [
        ("legacy_ratio_production", "R", 45, 13),
        ("absorption_ratio_shadow", "A", 47, 14),
    ],
)
def test_normalized_historical_rows_pass_profile_parity(
    tmp_path: Path,
    profile_id: str,
    fit_variable: str,
    expected_co2: int,
    expected_h2o: int,
) -> None:
    fixture = _fixture(tmp_path, profile_id)
    model = build_v1_5_historical_fit_evidence_normalizer(
        algorithm_profile_lineage_json=fixture["lineage"],
        historical_replay_evidence_json=fixture["replay"],
        route_baseline_attestation_json=fixture["attestation"],
    )
    outputs = write_v1_5_historical_fit_evidence_normalizer(model, tmp_path / "out")
    parity = build_v1_5_historical_fit_profile_parity(
        algorithm_profile_lineage_json=fixture["lineage"],
        fit_points_csv=outputs["fit_points"],
    )
    assert model["overall_status"] == "pass"
    assert model["fit_eligible_row_count"] == len(model["fit_points"])
    assert parity["overall_status"] == "pass"
    assert parity["fit_input_variable"] == fit_variable
    device = parity["device_summaries"][0]
    assert device["co2_observed_count"] == expected_co2
    assert device["h2o_wet_observed_count"] == expected_h2o
    assert device["h2o_dry_anchor_count"] >= 3
    assert model["opens_com_ports"] is False
    assert model["writes_coefficients"] is False
    assert len(model["source_paths"]["algorithm_profile_lineage_sha256"]) == 64
    assert len(model["source_paths"]["historical_replay_evidence_sha256"]) == 64
    assert len(model["source_paths"]["route_baseline_attestation_sha256"]) == 64


def test_normalizer_uses_ratio_T1_pressure_and_not_displayed_concentration(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, "legacy_ratio_production")
    model = build_v1_5_historical_fit_evidence_normalizer(
        algorithm_profile_lineage_json=fixture["lineage"],
        historical_replay_evidence_json=fixture["replay"],
        route_baseline_attestation_json=fixture["attestation"],
    )
    co2_row = next(row for row in model["fit_points"] if row["component"] == "co2")
    assert co2_row["fit_input_variable"] == "R"
    assert co2_row["fit_input_value"] == co2_row["ratio_r"]
    assert co2_row["fit_input_value"] != 9999.0
    assert co2_row["temperature_c"] != co2_row["temp_c"]
    assert co2_row["pressure_kpa"] == 101.2
    assert len(co2_row["source_samples_sha256"]) == 64


def test_missing_component_quality_is_preserved_as_review_gap(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, "legacy_ratio_production")
    h2o_point = next(point for point in fixture["points"] if point["route_kind"] == "h2o")
    (Path(h2o_point["point_path"]) / "formal_open_flow_data_quality_by_analyzer.csv").unlink()
    model = build_v1_5_historical_fit_evidence_normalizer(
        algorithm_profile_lineage_json=fixture["lineage"],
        historical_replay_evidence_json=fixture["replay"],
        route_baseline_attestation_json=fixture["attestation"],
    )
    affected = [row for row in model["fit_points"] if row["point_id"] == h2o_point["point_id"]]
    assert model["overall_status"] == "review_required"
    assert affected and affected[0]["fit_eligible"] is False
    assert "formal_h2o_quality_missing" in affected[0]["normalization_reasons"]


def test_formal_quality_sample_count_mismatch_blocks_the_row(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, "legacy_ratio_production")
    point = fixture["points"][0]
    quality_path = Path(point["point_path"]) / "formal_open_flow_data_quality_by_analyzer.csv"
    quality_rows = _read_csv(quality_path)
    quality_rows[0]["frame_count"] = "99"
    quality_rows[0]["usable_ratio_count"] = "99"
    _write_csv(quality_path, quality_rows)
    model = build_v1_5_historical_fit_evidence_normalizer(
        algorithm_profile_lineage_json=fixture["lineage"],
        historical_replay_evidence_json=fixture["replay"],
        route_baseline_attestation_json=fixture["attestation"],
    )
    affected = next(row for row in model["fit_points"] if row["point_id"] == point["point_id"])
    assert affected["fit_eligible"] is False
    assert "formal_co2_frame_count_mismatch" in affected["normalization_reasons"]
    assert "formal_co2_usable_ratio_count_mismatch" in affected["normalization_reasons"]


def test_duplicate_component_quality_rows_are_structural_blockers(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, "legacy_ratio_production")
    point = fixture["points"][0]
    quality_path = Path(point["point_path"]) / "formal_open_flow_data_quality_by_analyzer.csv"
    quality_rows = _read_csv(quality_path)
    quality_rows.append(dict(quality_rows[0]))
    _write_csv(quality_path, quality_rows)
    model = build_v1_5_historical_fit_evidence_normalizer(
        algorithm_profile_lineage_json=fixture["lineage"],
        historical_replay_evidence_json=fixture["replay"],
        route_baseline_attestation_json=fixture["attestation"],
    )
    reasons = {row["reason"] for row in model["structural_gaps"]}
    assert model["overall_status"] == "blocked"
    assert "duplicate_formal_quality_row:ga01:co2" in reasons


def test_duplicate_historical_root_keys_are_structural_blockers(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, "legacy_ratio_production")
    replay = json.loads(fixture["replay"].read_text(encoding="utf-8"))
    replay["evidence_roots"].append(dict(replay["evidence_roots"][0]))
    _write_json(fixture["replay"], replay)
    model = build_v1_5_historical_fit_evidence_normalizer(
        algorithm_profile_lineage_json=fixture["lineage"],
        historical_replay_evidence_json=fixture["replay"],
        route_baseline_attestation_json=fixture["attestation"],
    )
    reasons = {row["reason"] for row in model["structural_gaps"]}
    assert model["overall_status"] == "blocked"
    assert "duplicate_historical_evidence_root_key" in reasons


def test_absorption_normalizer_rechecks_current_r0_files(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, "absorption_ratio_shadow")
    lineage = json.loads(fixture["lineage"].read_text(encoding="utf-8"))
    Path(lineage["source_paths"]["h2o_r0_model_json"]).unlink()
    model = build_v1_5_historical_fit_evidence_normalizer(
        algorithm_profile_lineage_json=fixture["lineage"],
        historical_replay_evidence_json=fixture["replay"],
        route_baseline_attestation_json=fixture["attestation"],
    )
    reasons = {row["reason"] for row in model["structural_gaps"]}
    assert model["overall_status"] == "blocked"
    assert "h2o_r0_model_file_missing" in reasons
    assert "h2o_r0_evaluated_points_missing" not in reasons


def test_same_family_co2_and_h2o_roots_remain_separately_bound(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, "legacy_ratio_production")
    replay = json.loads(fixture["replay"].read_text(encoding="utf-8"))
    family_id = replay["evidence_roots"][0]["family_id"]
    old_root = Path(replay["evidence_roots"][0]["root_path"])
    route_roots = {
        "co2": old_root.parent / "legacy_0620_mature_co2",
        "h2o": old_root.parent / "legacy_0620_mature_h2o",
    }
    for root in route_roots.values():
        root.mkdir(parents=True)
    for point in replay["points"]:
        source = Path(point["point_path"])
        target = route_roots[point["route_kind"]] / source.name
        source.rename(target)
        point["point_path"] = str(target)
    replay["evidence_roots"] = [
        {
            "family_id": family_id,
            "route_kind": route,
            "root_path": str(root),
            "algorithm_profile_id": "legacy_ratio_production",
        }
        for route, root in route_roots.items()
    ]
    _write_json(fixture["replay"], replay)
    attestation = _write_json(
        tmp_path / "split_route_attestation.json",
        {
            "schema": "v1_5_historical_route_baseline_attestation_v1",
            "families": [
                {
                    "family_id": family_id,
                    "route_kind": route,
                    "root_path": str(root),
                    "route_baseline": "0620",
                    "fitting_baseline": "0613",
                    "status": "reviewed",
                    "reviewer": "offline-test-reviewer",
                    "reviewed_at": "2026-07-13T00:00:00+08:00",
                    "not_0624_or_migration_source": True,
                    "mature_contract": "0613_fit_0620_0621_route",
                    **_attestation_binding_fields(tmp_path, f"{family_id}_{route}"),
                }
                for route, root in route_roots.items()
            ],
        },
    )
    model = build_v1_5_historical_fit_evidence_normalizer(
        algorithm_profile_lineage_json=fixture["lineage"],
        historical_replay_evidence_json=fixture["replay"],
        route_baseline_attestation_json=attestation,
    )
    assert model["structural_blocker_count"] == 0
    assert set(model["family_route_baselines"]) == {f"{family_id}:co2", f"{family_id}:h2o"}


def test_unattested_root_is_a_structural_blocker(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, "legacy_ratio_production")
    replay = json.loads(fixture["replay"].read_text(encoding="utf-8"))
    replay["evidence_roots"][0]["root_path"] = str(tmp_path / "historical_20260624")
    _write_json(fixture["replay"], replay)
    model = build_v1_5_historical_fit_evidence_normalizer(
        algorithm_profile_lineage_json=fixture["lineage"],
        historical_replay_evidence_json=fixture["replay"],
    )
    reasons = {row["reason"] for row in model["structural_gaps"]}
    assert model["overall_status"] == "blocked"
    assert "route_baseline_reviewed_attestation_missing" in reasons


def test_attested_queue_hash_is_rechecked_at_consumption(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, "legacy_ratio_production")
    attestation = json.loads(fixture["attestation"].read_text(encoding="utf-8"))
    manifest = Path(attestation["families"][0]["queue_manifest_path"])
    manifest.write_text(manifest.read_text(encoding="utf-8") + "tampered\n", encoding="utf-8")
    model = build_v1_5_historical_fit_evidence_normalizer(
        algorithm_profile_lineage_json=fixture["lineage"],
        historical_replay_evidence_json=fixture["replay"],
        route_baseline_attestation_json=fixture["attestation"],
    )
    reasons = {row["reason"] for row in model["structural_gaps"]}
    assert model["overall_status"] == "blocked"
    assert "route_baseline_attestation_queue_manifest_hash_mismatch" in reasons


def test_reviewed_attestation_binds_an_unlabeled_root(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, "legacy_ratio_production")
    replay = json.loads(fixture["replay"].read_text(encoding="utf-8"))
    old_root = Path(replay["evidence_roots"][0]["root_path"])
    new_root = old_root.parent / "unlabeled_root"
    old_root.rename(new_root)
    replay["evidence_roots"][0]["root_path"] = str(new_root)
    for point in replay["points"]:
        point["point_path"] = str(new_root / Path(point["point_path"]).name)
    _write_json(fixture["replay"], replay)
    family_id = replay["evidence_roots"][0]["family_id"]
    attestation = _write_json(
        tmp_path / "attestation.json",
        {
            "schema": "v1_5_historical_route_baseline_attestation_v1",
            "families": [
                {
                    "family_id": family_id,
                    "route_kind": "mixed",
                    "root_path": replay["evidence_roots"][0]["root_path"],
                    "route_baseline": "0621",
                    "fitting_baseline": "0613",
                    "status": "reviewed",
                    "reviewer": "offline-test-reviewer",
                    "reviewed_at": "2026-07-13T00:00:00+08:00",
                        "not_0624_or_migration_source": True,
                        "mature_contract": "0613_fit_0620_0621_route",
                        **_attestation_binding_fields(tmp_path, "unlabeled_root"),
                    }
            ]
        },
    )
    model = build_v1_5_historical_fit_evidence_normalizer(
        algorithm_profile_lineage_json=fixture["lineage"],
        historical_replay_evidence_json=fixture["replay"],
        route_baseline_attestation_json=attestation,
    )
    assert model["structural_blocker_count"] == 0
    assert model["family_route_baselines"][f"{family_id}:mixed"] == "0621"


def test_cli_fails_closed_and_entrypoint_is_offline(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, "legacy_ratio_production")
    lineage = json.loads(fixture["lineage"].read_text(encoding="utf-8"))
    lineage["overall_status"] = "blocked"
    _write_json(fixture["lineage"], lineage)
    assert (
        main(
            [
                "--algorithm-profile-lineage-json",
                str(fixture["lineage"]),
                "--historical-replay-evidence-json",
                str(fixture["replay"]),
                "--route-baseline-attestation-json",
                str(fixture["attestation"]),
                "--output-dir",
                str(tmp_path / "cli"),
                "--fail-on-structural-blocker",
            ]
        )
        == 2
    )
    entry = classify_v1_5_entrypoint(
        ROOT / "src/gas_calibrator/tools/export_v1_5_historical_fit_evidence_normalizer.py",
        root=ROOT,
    )
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
