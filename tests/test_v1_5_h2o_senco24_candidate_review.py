import csv
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_h2o_senco24_candidate_review import main as cli_main
from gas_calibrator.validation.h2o_senco24_candidate_review import (
    H2OSenco24CandidateConfig,
    build_h2o_senco24_candidate_tables,
    write_h2o_senco24_candidate_report,
)


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _make_h2o_run(tmp_path: Path) -> Path:
    root = tmp_path / "h2o_run"
    queue_dir = root / "h2o_mt_no_write_r1"
    manifest_rows = []
    ratios = [0.21, 0.36, 0.29, 0.48, 0.62, 0.41, 0.72, 0.55]
    temps = [0.0, 10.0, 20.0, 30.0, 40.0, 5.0, 25.0, 35.0]
    for idx, (ratio, temp) in enumerate(zip(ratios, temps), start=1):
        point_name = f"p{idx:03d}_T{int(temp)}_HG{idx}_h2o"
        point = root / point_name
        target = 5.0 + 20.0 * ratio + 0.05 * temp
        manifest_rows.append(
            {
                "point_run_id": point_name,
                "point_id": f"h2o_point_{idx}",
                "temp_c": temp,
                "hgen_temp_c": temp,
                "hgen_rh_pct": 50.0,
                "reference_h2o_mmol": target - 0.25,
                "reference_dewpoint_c": temp - 10.0,
                "sample_role": "fit",
            }
        )
        rows = [
            {
                "Analyzer": "GA022",
                "ppm_H2O_Dew": target,
                "ppm_H2O": target + 0.05,
                "R_H2O": ratio,
                "R_H2O_dev": 0.0001,
                "T1": temp,
                "Temp": temp - 0.1,
                "Dew": temp - 10.0,
                "P": 1010.0,
                "BAR": 101.0,
                "ValidFrames": 10,
                "TotalFrames": 10,
                "FrameStatus": "all_usable",
                "PointIntegrity": "complete",
            },
            {
                "Analyzer": "GA051",
                "ppm_H2O_Dew": target,
                "ppm_H2O": 72.0,
                "R_H2O": ratio + 0.01,
                "R_H2O_dev": 0.0001,
                "T1": temp,
                "Temp": temp - 0.1,
                "Dew": temp - 10.0,
                "P": 1010.0,
                "BAR": 101.0,
                "ValidFrames": 10,
                "TotalFrames": 10,
                "FrameStatus": "all_usable",
                "PointIntegrity": "complete",
            },
        ]
        _write_csv(point / "分析仪汇总_水路_test.csv", rows)
        _write_csv(
            point / "samples_machine_readable.csv",
            [
                {
                    "sample_alignment_ok": "true",
                    "sampling_time_alignment_max_age_ms": 100.0,
                    "thermometer_cache_age_ms": 100.0,
                    "hgen_cache_age_ms": 100.0,
                    "dewpoint_sample_age_ms": 100.0,
                }
            ],
        )
    _write_csv(queue_dir / "queue_manifest.csv", manifest_rows)
    return root


def _make_physically_monotonic_h2o_run(tmp_path: Path) -> Path:
    root = _make_h2o_run(tmp_path)
    manifest_path = root / "h2o_mt_no_write_r1" / "queue_manifest.csv"
    with manifest_path.open("r", encoding="utf-8-sig", newline="") as handle:
        manifest_rows = [dict(row) for row in csv.DictReader(handle)]

    reference_by_point: dict[str, float] = {}
    for point_dir in sorted(path for path in root.glob("p*") if path.is_dir()):
        summary_path = next(
            path for path in sorted(point_dir.glob("*.csv")) if path.name != "samples_machine_readable.csv"
        )
        with summary_path.open("r", encoding="utf-8-sig", newline="") as handle:
            rows = [dict(row) for row in csv.DictReader(handle)]
        base_ratio = float(rows[0]["R_H2O"])
        chamber_temp_c = float(rows[0]["T1"])
        reference = 30.0 - 28.0 * base_ratio + 0.035 * chamber_temp_c
        reference_by_point[point_dir.name] = reference
        for row in rows:
            row["ppm_H2O_Dew"] = reference
            row["ppm_H2O"] = reference + 0.05
        _write_csv(summary_path, rows)

    for row in manifest_rows:
        row["reference_h2o_mmol"] = reference_by_point[str(row["point_run_id"])]
    _write_csv(manifest_path, manifest_rows)
    return root


def _make_co2_dry_anchor_run(tmp_path: Path) -> Path:
    root = tmp_path / "co2_dry_anchor_run"
    for idx, temp in enumerate((0.0, 20.0), start=1):
        point_name = f"p{idx:03d}_T{int(temp)}_0ppm_fit"
        point = root / point_name
        _write_csv(
            point / "分析仪汇总_气路_test.csv",
            [
                {
                    "Analyzer": "GA01",
                    "ppm_H2O_Dew": 0.35 + idx * 0.1,
                    "ppm_H2O": 2.1 + idx * 0.2,
                    "R_H2O": 0.18 + idx * 0.01,
                    "R_H2O_dev": 0.0001,
                    "T1": temp + 0.2,
                    "Temp": temp,
                    "Dew": -32.0 + idx,
                    "P": 1012.0,
                    "BAR": 101.2,
                    "ValidFrames": 10,
                    "TotalFrames": 10,
                    "FrameStatus": "全部可用",
                    "PointIntegrity": "完整",
                }
            ],
        )
        _write_csv(
            point / "samples_machine_readable.csv",
            [
                {
                    "sample_alignment_ok": "true",
                    "sampling_time_alignment_max_age_ms": 100.0,
                    "thermometer_cache_age_ms": 100.0,
                    "hgen_cache_age_ms": 100.0,
                    "dewpoint_sample_age_ms": 100.0,
                    "ga01_analyzer_device_id": "022",
                }
            ],
        )
    return root


def _make_mapped_h2o_single_point(tmp_path: Path) -> Path:
    root = tmp_path / "mapped_h2o_single_point"
    _write_csv(
        root / "分析仪汇总_水路_test.csv",
        [
            {
                "Analyzer": "GA01",
                "ppm_H2O_Dew": 12.3,
                "ppm_H2O": 12.0,
                "R_H2O": 0.42,
                "R_H2O_dev": 0.0001,
                "T1": 20.0,
                "Temp": 19.9,
                "Dew": 5.0,
                "P": 1010.0,
                "BAR": 101.0,
                "ValidFrames": 10,
                "TotalFrames": 10,
                "FrameStatus": "all_usable",
                "PointIntegrity": "complete",
            }
        ],
    )
    _write_csv(
        root / "samples_machine_readable.csv",
        [
            {
                "sample_alignment_ok": "true",
                "sampling_time_alignment_max_age_ms": 100.0,
                "thermometer_cache_age_ms": 100.0,
                "hgen_cache_age_ms": 100.0,
                "dewpoint_sample_age_ms": 100.0,
                "ga01_analyzer_device_id": "023",
            }
        ],
    )
    return root


def _make_h2o_state_transfer_summary(tmp_path: Path) -> Path:
    path = tmp_path / "h2o_state_transfer" / "h2o_s24_post_s6_state_delta_by_device_point.csv"
    _write_csv(
        path,
        [
            {
                "device_id": "022",
                "channel_prefix_s24": "ga01",
                "point_id": "p001_T20_HG20C_30RH_h2o",
                "h2o_ratio_f_mean_delta_post_minus_s24": 0.0008,
                "chamber_temp_c_mean_delta_post_minus_s24": -0.7,
                "live_reference_h2o_mmol_delta_post_minus_s24": -0.05,
                "senco24_replay_h2o_mmol_mean_delta_post_minus_s24": -0.31,
                "raw_replay_delta_minus_reference_delta_mmol": -0.26,
                "post_existing_s6_error_mmol": -0.20,
                "post_existing_s6_abs_rel_pct": 2.6,
            }
        ],
    )
    return path


def _make_pressure_qc_fallback_h2o_run(tmp_path: Path) -> Path:
    root = tmp_path / "pressure_qc_fallback_h2o_run"
    queue_dir = root / "h2o_mt_no_write_r1"
    manifest_rows = []
    ratios = [0.22, 0.31, 0.39, 0.46, 0.54, 0.61, 0.69, 0.75]
    temps = [0.0, 10.0, 20.0, 30.0, 40.0, 5.0, 25.0, 35.0]
    for idx, (ratio, temp) in enumerate(zip(ratios, temps), start=1):
        point_name = f"p{idx:03d}_T{int(temp)}_HG{idx}_h2o"
        point = root / point_name
        target = 3.0 + 18.0 * ratio + 0.03 * temp
        manifest_rows.append(
            {
                "point_run_id": point_name,
                "point_id": f"h2o_point_{idx}",
                "temp_c": temp,
                "hgen_temp_c": temp,
                "hgen_rh_pct": 50.0,
                "reference_h2o_mmol": target,
                "reference_dewpoint_c": temp - 12.0,
                "sample_role": "fit",
            }
        )
        _write_csv(
            point / "分析仪汇总_水路_test.csv",
            [
                {
                    "Analyzer": "GA06",
                    "ppm_H2O_Dew": target,
                    "ppm_H2O": "",
                    "R_H2O": "",
                    "R_H2O_dev": "",
                    "T1": "",
                    "Temp": temp - 0.2,
                    "Dew": temp - 12.0,
                    "P": 1010.0,
                    "BAR": 200.0,
                    "ValidFrames": 0,
                    "TotalFrames": 10,
                    "FrameStatus": "only_abnormal_frames",
                    "PointIntegrity": "pressure_qc_failed",
                }
            ],
        )
        _write_csv(
            point / "samples_machine_readable.csv",
            [
                {
                    "sample_alignment_ok": "true",
                    "sampling_time_alignment_max_age_ms": 100.0,
                    "thermometer_cache_age_ms": 100.0,
                    "hgen_cache_age_ms": 100.0,
                    "dewpoint_sample_age_ms": 100.0,
                    "ga06_analyzer_device_id": "090",
                    "ga06_h2o_mmol": target + 0.2,
                    "ga06_h2o_ratio_f": ratio,
                    "ga06_h2o_ratio_raw": ratio + 0.0002,
                    "ga06_chamber_temp_c": temp + 0.1,
                    "ga06_case_temp_c": temp + 0.3,
                    "ga06_pressure_kpa": 200.0,
                    "ga06_mode2_qc_status": "fail",
                    "ga06_mode2_qc_reason": "P_kPa>150(200)",
                    "ga06_frame_status": "only_abnormal_frames",
                }
            ],
        )
    _write_csv(queue_dir / "queue_manifest.csv", manifest_rows)
    return root


def test_h2o_senco24_candidate_uses_dewpoint_reference_and_blocks_pinned_output(tmp_path):
    run_dir = _make_h2o_run(tmp_path)

    tables, context = build_h2o_senco24_candidate_tables(
        run_dir=run_dir,
        cfg=H2OSenco24CandidateConfig(min_points=8, fit_max_abs_error_mmol=0.1),
    )

    assert context["point_count"] == 8
    policies = {row["analyzer_device_id"]: row for row in tables["h2o_senco24_device_policy"]}
    assert policies["022"]["reference_target_source"] == "ppm_H2O_Dew_from_dewpoint_meter_and_COM22_pressure"
    assert policies["022"]["candidate_status"] == "candidate_fit_ready_requires_independent_verification"
    assert policies["022"]["fit_max_error_mmol"] < 0.1
    assert policies["022"]["design_max_relative_error_pct"] == 2.0
    assert policies["022"]["fit_design_qc"] == "pass"
    assert policies["022"]["fit_max_abs_relative_error_pct"] < 2.0
    assert policies["051"]["candidate_status"] == "candidate_ratio_fit_available_but_final_output_blocked"
    assert policies["051"]["final_output_pinned"] is True
    assert "P" in policies["022"]["frozen_terms"]
    diagnostics = {row["analyzer_device_id"]: row for row in tables["h2o_senco24_output_diagnostics"]}
    assert diagnostics["051"]["diagnosis"] == "final_h2o_output_pinned_but_ratio_model_valid"
    assert "GETCO6" in diagnostics["051"]["next_safe_action"]

    coeff_terms = {
        row["term"]
        for row in tables["h2o_senco24_coefficients"]
        if row["analyzer_device_id"] == "022"
    }
    assert coeff_terms == {"intercept", "R", "R2", "R3", "T", "T2", "RT"}
    assert all(row["senco_channel"] in {"SENCO2", "SENCO4"} for row in tables["h2o_senco24_coefficients"])
    payload = [row for row in tables["h2o_senco24_payload_preview"] if row["analyzer_device_id"] == "022"][0]
    assert payload["senco2_command_preview"].startswith("SENCO2,YGAS,FFF,")
    assert payload["senco4_command_preview"].startswith("SENCO4,YGAS,FFF,")
    assert payload["auto_write_allowed"] is False


def test_h2o_senco24_candidate_blocks_nontransferable_s24_state(tmp_path):
    run_dir = _make_h2o_run(tmp_path)
    transfer_csv = _make_h2o_state_transfer_summary(tmp_path)

    tables, context = build_h2o_senco24_candidate_tables(
        run_dir=run_dir,
        cfg=H2OSenco24CandidateConfig(
            min_points=8,
            fit_max_abs_error_mmol=0.1,
            state_transfer_summary_csv=transfer_csv,
            state_transfer_max_raw_excess_shift_mmol=0.1,
            state_transfer_max_post_s6_relative_error_pct=2.0,
        ),
    )

    assert context["state_transfer_source"] == str(transfer_csv.resolve())
    policies = {row["analyzer_device_id"]: row for row in tables["h2o_senco24_device_policy"]}
    policy = policies["022"]
    assert policy["candidate_status"] == "blocked"
    assert "senco24_raw_state_transfer_excess_shift" in policy["blocked_reasons"]
    assert "post_s6_state_transfer_relative_error_exceeds_limit" in policy["blocked_reasons"]
    assert policy["state_transfer_gate"].startswith("fail_")
    assert policy["state_transfer_worst_point_id"] == "p001_T20_HG20C_30RH_h2o"
    assert policy["state_transfer_max_abs_raw_excess_shift_mmol"] == 0.26
    diagnostics = {row["analyzer_device_id"]: row for row in tables["h2o_senco24_output_diagnostics"]}
    assert diagnostics["022"]["diagnosis"] == "senco24_state_transfer_failed"
    contract_topics = {row["topic"] for row in tables["h2o_senco24_measurement_contract"]}
    assert "state_transfer_gate" in contract_topics


def test_h2o_senco24_candidate_can_use_pressure_qc_failed_ratio_evidence_when_explicitly_allowed(tmp_path):
    run_dir = _make_pressure_qc_fallback_h2o_run(tmp_path)

    tables, context = build_h2o_senco24_candidate_tables(
        run_dir=run_dir,
        cfg=H2OSenco24CandidateConfig(
            min_points=8,
            allow_pressure_qc_failed_device_ids=("090",),
        ),
    )

    assert context["allow_pressure_qc_failed_device_ids"] == ["090"]
    policies = {row["analyzer_device_id"]: row for row in tables["h2o_senco24_device_policy"]}
    assert policies["090"]["complete_point_count"] == 8
    assert policies["090"]["pressure_qc_override_allowed"] is True
    assert policies["090"]["pressure_qc_override_point_count"] == 8
    assert "pressure_qc_failed_overridden_for_component_fit:8" in policies["090"]["warning_reasons"]
    residuals = [
        row for row in tables["h2o_senco24_residuals"] if row.get("analyzer_device_id") == "090"
    ]
    assert len(residuals) == 8
    assert all(row["pressure_qc_overridden_for_component_fit"] is True for row in residuals)
    assert all("P_kPa>150" in row["sample_mode2_qc_reason"] for row in residuals)


def test_h2o_senco24_candidate_can_use_digital_thermometer_as_repair_fit_temperature(tmp_path):
    run_dir = _make_h2o_run(tmp_path)

    tables, context = build_h2o_senco24_candidate_tables(
        run_dir=run_dir,
        cfg=H2OSenco24CandidateConfig(
            min_points=8,
            fit_temperature_source="digital_thermometer",
        ),
    )

    assert context["fit_temperature_source"] == "digital_thermometer"
    policies = {row["analyzer_device_id"]: row for row in tables["h2o_senco24_device_policy"]}
    assert policies["022"]["digital_thermometer_temperature_fit_point_count"] == 8
    assert "fit_temperature_source_digital_thermometer:8" in policies["022"]["warning_reasons"]
    residual = next(
        row
        for row in tables["h2o_senco24_residuals"]
        if row.get("analyzer_device_id") == "022" and row.get("point_run_id") == "p001_T0_HG1_h2o"
    )
    assert residual["temperature_source_for_fit"] == "digital_thermometer_temp_c"
    assert residual["chamber_temp_c"] == -0.1
    assert residual["analyzer_chamber_temp_c_raw"] == 0.0


def test_h2o_senco24_candidate_requires_getco6_snapshot_when_layer_review_required(tmp_path):
    run_dir = _make_h2o_run(tmp_path)

    tables, _ = build_h2o_senco24_candidate_tables(
        run_dir=run_dir,
        cfg=H2OSenco24CandidateConfig(
            min_points=8,
            require_component_snapshot_for_layer_review=True,
        ),
    )

    policies = {row["analyzer_device_id"]: row for row in tables["h2o_senco24_device_policy"]}
    assert policies["022"]["GETCO6_layer_status"] == "missing_assume_neutral"
    assert policies["022"]["senco24_write_candidate"] is False
    assert policies["022"]["senco6_separate_review_required"] is True
    assert policies["022"]["require_component_snapshot_for_layer_review"] is True
    assert (
        "GETCO6_missing_component_snapshot_separate_layer_review_required"
        in policies["022"]["warning_reasons"]
    )
    diagnostics = {row["analyzer_device_id"]: row for row in tables["h2o_senco24_output_diagnostics"]}
    assert diagnostics["022"]["formal_acceptance_status"] == (
        "candidate_ready_requires_layer_contract_review_and_independent_verification"
    )


def test_h2o_senco24_candidate_can_include_dewpoint_based_dry_anchors(tmp_path):
    run_dir = _make_h2o_run(tmp_path)
    dry_anchor_run = _make_co2_dry_anchor_run(tmp_path)

    tables, context = build_h2o_senco24_candidate_tables(
        run_dir=run_dir,
        cfg=H2OSenco24CandidateConfig(
            min_points=8,
            fit_max_abs_error_mmol=10.0,
            dry_anchor_roots=(str(dry_anchor_run),),
        ),
    )

    assert context["dry_anchor_input_count"] == 2
    point_inputs = tables["h2o_senco24_point_inputs"]
    dry_anchors = [row for row in point_inputs if row["sample_role"] == "dry_anchor"]
    assert len(dry_anchors) == 2
    assert {row["analyzer_device_id"] for row in dry_anchors} == {"022"}
    assert all(row["reference_h2o_mmol"] not in (0, 0.0, "0") for row in dry_anchors)
    assert {row["h2o_anchor_class"] for row in dry_anchors} == {"dry_gas_anchor"}
    assert {row["reference_source"] for row in dry_anchors} == {
        "dewpoint_meter_plus_com22_pressure_dry_gas_anchor"
    }

    policies = {row["analyzer_device_id"]: row for row in tables["h2o_senco24_device_policy"]}
    assert policies["022"]["complete_dry_anchor_count"] == 2
    assert policies["022"]["complete_wet_point_count"] == 8
    assert policies["051"]["complete_dry_anchor_count"] == 0


def test_h2o_senco24_candidate_can_filter_dry_anchors_by_temperature_and_relative_objective(tmp_path):
    run_dir = _make_h2o_run(tmp_path)
    dry_anchor_run = _make_co2_dry_anchor_run(tmp_path)

    tables, context = build_h2o_senco24_candidate_tables(
        run_dir=run_dir,
        cfg=H2OSenco24CandidateConfig(
            min_points=8,
            fit_max_abs_error_mmol=10.0,
            dry_anchor_roots=(str(dry_anchor_run),),
            dry_anchor_max_temp_c=0.0,
            fit_objective="relative_mmol_floor",
        ),
    )

    assert context["dry_anchor_input_count"] == 1
    assert context["dry_anchor_max_temp_c"] == 0.0
    assert context["fit_objective"] == "relative_mmol_floor"
    dry_anchors = [
        row for row in tables["h2o_senco24_point_inputs"] if row["sample_role"] == "dry_anchor"
    ]
    assert len(dry_anchors) == 1
    assert dry_anchors[0]["temp_set_c"] == 0.0
    policies = {row["analyzer_device_id"]: row for row in tables["h2o_senco24_device_policy"]}
    assert policies["022"]["complete_dry_anchor_count"] == 1
    assert policies["022"]["fit_objective"] == "relative_mmol_floor"


def test_h2o_senco24_candidate_supports_monotonic_minimax_relative_objective(tmp_path):
    run_dir = _make_physically_monotonic_h2o_run(tmp_path)

    tables, context = build_h2o_senco24_candidate_tables(
        run_dir=run_dir,
        cfg=H2OSenco24CandidateConfig(
            min_points=8,
            fit_max_abs_error_mmol=1.0,
            design_max_relative_error_pct=2.0,
            relative_error_min_reference_mmol=1.0,
            fit_objective="minimax_relative_mmol_floor_monotonic",
        ),
    )

    assert context["fit_objective"] == "minimax_relative_mmol_floor_monotonic"
    policies = {row["analyzer_device_id"]: row for row in tables["h2o_senco24_device_policy"]}
    for device_id in ("022", "051"):
        policy = policies[device_id]
        assert policy["fit_solver"] == "scipy_highs_linear_program"
        assert policy["fit_basis"] == "centered_R_T_transformed_to_firmware_absolute_terms"
        assert policy["fit_objective_bound_pct_pre_firmware_quantization"] < 1.0e-6
        assert policy["coefficients_quantized_for_firmware"] is True
        assert policy["fit_max_abs_relative_error_pct"] < 0.01
        assert policy["monotonic_decreasing_over_fit_envelope"] is True
        assert policy["monotonic_derivative_violation_count"] == 0
        assert policy["maximum_d_concentration_d_ratio"] < 0.0
        assert policy["fit_design_qc"] == "pass"

    payloads = tables["h2o_senco24_payload_preview"]
    assert payloads
    assert all(row["auto_write_allowed"] is False for row in payloads)
    assert all(row["fit_objective"] == "minimax_relative_mmol_floor_monotonic" for row in payloads)


def test_h2o_senco24_design_failure_is_not_hidden_by_pinned_output(tmp_path):
    run_dir = _make_h2o_run(tmp_path)
    cfg = H2OSenco24CandidateConfig(
        min_points=8,
        fit_max_abs_error_mmol=100.0,
        design_max_relative_error_pct=2.0,
        relative_error_min_reference_mmol=1.0,
        fit_objective="minimax_relative_mmol_floor_monotonic",
    )

    tables, _ = build_h2o_senco24_candidate_tables(
        run_dir=run_dir,
        cfg=cfg,
    )

    policy = {row["analyzer_device_id"]: row for row in tables["h2o_senco24_device_policy"]}[
        "051"
    ]
    assert policy["final_output_pinned"] is True
    assert policy["fit_max_abs_relative_error_pct"] > 2.0
    assert policy["candidate_status"] == "candidate_fit_review_required"

    outputs = write_h2o_senco24_candidate_report(
        run_dir=run_dir,
        output_dir=tmp_path / "design_failure_sidecar",
        cfg=cfg,
    )
    sidecar = json.loads(outputs["database_sidecar"].read_text(encoding="utf-8"))
    qc_row = next(
        row
        for row in sidecar["suggested_rows"]
        if row["record_key"] == "h2o_senco2_senco4_qc_051"
    )
    assert qc_row["status"] == "fail"
    assert sidecar["connects_postgresql"] is False


def test_h2o_senco24_cli_accepts_monotonic_minimax_relative_objective(tmp_path):
    run_dir = _make_physically_monotonic_h2o_run(tmp_path)
    output_dir = tmp_path / "minimax_cli"

    rc = cli_main(
        [
            "--run-dir",
            str(run_dir),
            "--output-dir",
            str(output_dir),
            "--fit-objective",
            "minimax_relative_mmol_floor_monotonic",
            "--relative-error-min-reference-mmol",
            "1.0",
        ]
    )

    assert rc == 0
    metadata = json.loads(
        (output_dir / "h2o_senco24_candidate_review_meta.json").read_text(encoding="utf-8")
    )
    assert metadata["config_summary"]["fit_objective"] == (
        "minimax_relative_mmol_floor_monotonic"
    )
    sidecar = json.loads(
        (output_dir / "h2o_senco24_database_sidecar.json").read_text(encoding="utf-8")
    )
    assert sidecar["no_write"] is True
    assert sidecar["connects_postgresql"] is False


def test_h2o_senco24_candidate_merges_extra_h2o_root_and_maps_real_device_id(tmp_path):
    run_dir = _make_h2o_run(tmp_path)
    extra_h2o = _make_mapped_h2o_single_point(tmp_path)

    tables, context = build_h2o_senco24_candidate_tables(
        run_dir=run_dir,
        cfg=H2OSenco24CandidateConfig(
            min_points=1,
            fit_max_abs_error_mmol=10.0,
            additional_h2o_roots=(str(extra_h2o),),
        ),
    )

    assert str(extra_h2o.resolve()) in context["additional_h2o_roots"]
    point_inputs = tables["h2o_senco24_point_inputs"]
    mapped = [row for row in point_inputs if row["h2o_source_root"] == str(extra_h2o.resolve())]
    assert len(mapped) == 1
    assert mapped[0]["analyzer"] == "GA01"
    assert mapped[0]["analyzer_device_id"] == "023"
    policies = {row["analyzer_device_id"]: row for row in tables["h2o_senco24_device_policy"]}
    assert "wet_points<3" in policies["023"]["blocked_reasons"]


def test_h2o_senco24_candidate_enforces_2pct_design_relative_error(tmp_path):
    run_dir = _make_h2o_run(tmp_path)
    summary_path = next(
        path for path in sorted(run_dir.glob("p001*/*.csv")) if path.name != "samples_machine_readable.csv"
    )
    with summary_path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = [dict(row) for row in csv.DictReader(handle)]
    rows[0]["ppm_H2O_Dew"] = float(rows[0]["ppm_H2O_Dew"]) + 1.0
    _write_csv(summary_path, rows)

    tables, _ = build_h2o_senco24_candidate_tables(
        run_dir=run_dir,
        cfg=H2OSenco24CandidateConfig(
            min_points=8,
            fit_max_abs_error_mmol=10.0,
            design_max_relative_error_pct=0.000001,
        ),
    )

    policies = {row["analyzer_device_id"]: row for row in tables["h2o_senco24_device_policy"]}
    assert policies["022"]["candidate_status"] == "candidate_fit_review_required"
    assert policies["022"]["fit_design_qc"] == "review"
    assert policies["022"]["design_max_relative_error_pct"] == 0.000001


def test_h2o_senco24_candidate_supports_manual_firmware_block(tmp_path):
    run_dir = _make_h2o_run(tmp_path)

    tables, context = build_h2o_senco24_candidate_tables(
        run_dir=run_dir,
        cfg=H2OSenco24CandidateConfig(
            min_points=8,
            manual_device_block_reasons={"022": "firmware_upgrade_required"},
        ),
    )

    assert context["run_status"] == "blocked"
    policy = {row["analyzer_device_id"]: row for row in tables["h2o_senco24_device_policy"]}["022"]
    assert policy["candidate_status"] == "blocked"
    assert "manual_device_block:firmware_upgrade_required" in policy["blocked_reasons"]
    coeffs = [row for row in tables["h2o_senco24_coefficients"] if row["analyzer_device_id"] == "022"]
    payloads = [row for row in tables["h2o_senco24_payload_preview"] if row["analyzer_device_id"] == "022"]
    assert len(coeffs) == 7
    assert len(payloads) == 1
    assert payloads[0]["auto_write_allowed"] is False
    diagnostics = {row["analyzer_device_id"]: row for row in tables["h2o_senco24_output_diagnostics"]}
    assert diagnostics["022"]["diagnosis"] == "manual_device_block"
    assert diagnostics["022"]["likely_cause"] == "firmware_upgrade_required"


def test_h2o_senco24_candidate_blocks_when_factory_signal_health_requires_review(tmp_path):
    run_dir = _make_h2o_run(tmp_path)
    factory_summary = tmp_path / "factory_signal_health_summary.csv"
    _write_csv(
        factory_summary,
        [
            {
                "device_id": "022",
                "point_count": 2,
                "review_point_count": 0,
                "blocking_point_count": 0,
                "high_ref_point_count": 0,
                "candidate_gate": "review_insufficient_factory_signal_coverage",
            },
            {
                "device_id": "051",
                "point_count": 8,
                "review_point_count": 0,
                "blocking_point_count": 0,
                "high_ref_point_count": 0,
                "candidate_gate": "pass_factory_signal_health",
            },
        ],
    )

    tables, context = build_h2o_senco24_candidate_tables(
        run_dir=run_dir,
        cfg=H2OSenco24CandidateConfig(
            min_points=8,
            factory_signal_health_summary_csv=factory_summary,
        ),
    )

    assert context["run_status"] != "fit_ready_requires_independent_verification"
    policies = {row["analyzer_device_id"]: row for row in tables["h2o_senco24_device_policy"]}
    assert policies["022"]["candidate_status"] == "blocked"
    assert policies["022"]["factory_signal_health_gate"] == "review_insufficient_factory_signal_coverage"
    assert "factory_signal_health_review:review_insufficient_factory_signal_coverage" in policies["022"]["blocked_reasons"]
    assert policies["051"]["factory_signal_health_gate"] == "pass_factory_signal_health"


def test_h2o_senco24_candidate_preserves_manual_point_block_as_rejected_evidence(tmp_path):
    run_dir = _make_h2o_run(tmp_path)

    tables, _ = build_h2o_senco24_candidate_tables(
        run_dir=run_dir,
        cfg=H2OSenco24CandidateConfig(
            min_points=7,
            fit_max_abs_error_mmol=0.1,
            manual_point_block_reasons={
                "022:p001_T0_HG1_h2o": "first_low_h2o_anchor_requires_makeup_review",
            },
        ),
    )

    policies = {row["analyzer_device_id"]: row for row in tables["h2o_senco24_device_policy"]}
    assert policies["022"]["complete_point_count"] == 7
    assert policies["022"]["rejected_point_count"] == 1
    assert "manual_point_blocks:1" in policies["022"]["warning_reasons"]

    rejected = [
        row
        for row in tables["h2o_senco24_residuals"]
        if row.get("analyzer_device_id") == "022" and row.get("residual_role") == "rejected_input"
    ]
    assert len(rejected) == 1
    assert rejected[0]["point_run_id"] == "p001_T0_HG1_h2o"
    assert "manual_point_block:first_low_h2o_anchor_requires_makeup_review" in rejected[0]["reject_reasons"]


def test_h2o_senco24_candidate_uses_getco_snapshot_to_route_pinned_output(tmp_path):
    run_dir = _make_h2o_run(tmp_path)

    tables, _ = build_h2o_senco24_candidate_tables(
        run_dir=run_dir,
        cfg=H2OSenco24CandidateConfig(
            min_points=8,
            fit_max_abs_error_mmol=0.1,
            component_snapshot={
                "051": {
                    "GETCO2_before": [1288.01, 0.0, 0.766182, 0.295489, -0.0620835, 0.0],
                    "GETCO4_before": [0.0, 0.0, 0.0, 1.0, 0.0, 0.0],
                    "GETCO6_before": [0.0, 1.0],
                }
            },
        ),
    )

    diagnostics = {row["analyzer_device_id"]: row for row in tables["h2o_senco24_output_diagnostics"]}
    assert diagnostics["051"]["diagnosis"] == "final_h2o_output_pinned_with_neutral_senco6"
    assert diagnostics["051"]["GETCO6_neutral"] is True
    assert "SENCO2_SENCO4" in diagnostics["051"]["next_safe_action"]
    assert "do_not_CLEARSENCO6" in diagnostics["051"]["next_safe_action"]


def test_h2o_senco24_candidate_marks_prewrite_pin_resolved_by_postwrite_verification(tmp_path):
    run_dir = _make_h2o_run(tmp_path)

    tables, _ = build_h2o_senco24_candidate_tables(
        run_dir=run_dir,
        cfg=H2OSenco24CandidateConfig(
            min_points=8,
            fit_max_abs_error_mmol=0.1,
            component_snapshot={
                "051": {
                    "GETCO2_before": [1288.01, 0.0, 0.766182, 0.295489, -0.0620835, 0.0],
                    "GETCO4_before": [0.0, 0.0, 0.0, 1.0, 0.0, 0.0],
                    "GETCO6_before": [0.0, 1.0],
                }
            },
            postwrite_verified_device_ids=("051",),
            postwrite_verification_artifacts=("h2o_post_senco24_write_r3_verification_summary.md",),
        ),
    )

    policies = {row["analyzer_device_id"]: row for row in tables["h2o_senco24_device_policy"]}
    assert policies["051"]["candidate_status"] == "candidate_fit_ready_with_warnings_requires_independent_verification"
    assert policies["051"]["final_output_pinned"] is True
    assert policies["051"]["postwrite_verified"] is True
    assert "prewrite_final_h2o_output_pinned_resolved_by_postwrite_verification" in policies["051"]["warning_reasons"]
    diagnostics = {row["analyzer_device_id"]: row for row in tables["h2o_senco24_output_diagnostics"]}
    assert diagnostics["051"]["diagnosis"] == "prewrite_final_h2o_output_pinned_resolved_by_postwrite_verification"
    assert diagnostics["051"]["formal_acceptance_status"] == "postwrite_verified_for_review_scope"


def test_h2o_senco24_candidate_accounts_for_nonneutral_getco6_before_main_write(tmp_path):
    run_dir = _make_h2o_run(tmp_path)

    tables, _ = build_h2o_senco24_candidate_tables(
        run_dir=run_dir,
        cfg=H2OSenco24CandidateConfig(
            min_points=8,
            fit_max_abs_error_mmol=0.1,
            component_snapshot={
                "022": {
                    "GETCO2_before": [1.0, 2.0, 3.0, 4.0, 0.0, 0.0],
                    "GETCO4_before": [0.1, 0.2, 0.3, 0.0, 0.0, 0.0],
                    "GETCO6_before": [1.0, 1.0],
                }
            },
        ),
    )

    policies = {row["analyzer_device_id"]: row for row in tables["h2o_senco24_device_policy"]}
    assert policies["022"]["candidate_status"] == "candidate_fit_ready_with_warnings_requires_independent_verification"
    assert policies["022"]["blocked_reasons"] == ""
    assert (
        "existing_GETCO6_nonneutral_final_affine_layer_requires_separate_review_separate_layer_review_required"
        in policies["022"]["warning_reasons"]
    )
    assert policies["022"]["fit_strategy"] == "direct_reference_target_fit_SENCO2_SENCO4_SENCO6_separate"
    assert policies["022"]["senco24_write_candidate"] is False
    assert policies["022"]["senco6_separate_review_required"] is True
    assert policies["022"]["GETCO6_C0"] == 1.0
    assert policies["022"]["GETCO6_C1"] == 1.0
    diagnostics = {row["analyzer_device_id"]: row for row in tables["h2o_senco24_output_diagnostics"]}
    assert diagnostics["022"]["diagnosis"] == "ratio_temperature_candidate_fit_valid_with_separate_senco6_review_required"
    assert diagnostics["022"]["formal_acceptance_status"] == (
        "candidate_ready_requires_layer_contract_review_and_independent_verification"
    )


def test_h2o_senco24_writer_and_cli_create_no_write_artifacts(tmp_path):
    run_dir = _make_h2o_run(tmp_path)
    dry_anchor_run = _make_co2_dry_anchor_run(tmp_path)
    extra_h2o = _make_mapped_h2o_single_point(tmp_path)
    output_dir = tmp_path / "out"

    outputs = write_h2o_senco24_candidate_report(run_dir=run_dir, output_dir=output_dir)

    assert outputs["workbook"].exists()
    assert outputs["h2o_senco24_device_policy_csv"].exists()
    assert outputs["markdown"].exists()
    sidecar = json.loads(outputs["database_sidecar"].read_text(encoding="utf-8"))
    assert sidecar["no_write"] is True
    assert sidecar["controls_water_or_gas_routes"] is False
    assert "coefficient_candidates" in sidecar["database_target_tables"]

    cli_output = tmp_path / "cli"
    cli_snapshot = tmp_path / "component_snapshot.json"
    cli_snapshot.write_text(
        json.dumps({"051": {"GETCO6_before": [0.0, 1.0]}}, ensure_ascii=False),
        encoding="utf-8",
    )
    rc = cli_main(
        [
            "--run-dir",
            str(run_dir),
            "--dry-anchor-run-dir",
            str(dry_anchor_run),
            "--additional-h2o-run-dir",
            str(extra_h2o),
            "--output-dir",
            str(cli_output),
            "--manual-device-block",
            "022=firmware_upgrade_required",
            "--manual-point-block",
            "051:p001_T0_HG1_h2o=operator_marked_diagnostic",
            "--component-snapshot-json",
            str(cli_snapshot),
            "--require-component-snapshot-for-layer-review",
            "--postwrite-verified-device-id",
            "051",
            "--postwrite-verification-artifact",
            "h2o_post_senco24_write_r3_verification_summary.md",
            "--allow-pressure-qc-failed-device-id",
            "090",
            "--fit-temperature-source",
            "digital_thermometer",
            "--dry-anchor-max-temp-c",
            "0",
            "--fit-objective",
            "relative_mmol_floor",
        ]
    )
    assert rc == 0
    assert (cli_output / "h2o_senco24_candidate_review.xlsx").exists()
    metadata = json.loads((cli_output / "h2o_senco24_candidate_review_meta.json").read_text(encoding="utf-8"))
    assert metadata["config_summary"]["dry_anchor_input_count"] == 1
    assert metadata["config_summary"]["dry_anchor_max_temp_c"] == 0.0
    assert metadata["config_summary"]["fit_objective"] == "relative_mmol_floor"
    assert str(extra_h2o.resolve()) in metadata["config_summary"]["additional_h2o_roots"]
    assert metadata["config_summary"]["min_wet_points"] == 3
    assert metadata["config_summary"]["postwrite_verified_device_ids"] == ["051"]
    assert metadata["config_summary"]["postwrite_verification_artifacts"] == [
        "h2o_post_senco24_write_r3_verification_summary.md"
    ]
    assert metadata["config_summary"]["allow_pressure_qc_failed_device_ids"] == ["090"]
    assert metadata["config_summary"]["fit_temperature_source"] == "digital_thermometer"
    assert metadata["config_summary"]["require_component_snapshot_for_layer_review"] is True
