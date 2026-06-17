import csv
import json

import pytest

from gas_calibrator.tools.export_v1_5_post_run_coefficient_executor import (
    main as export_executor_main,
)
from gas_calibrator.validation.v1_5_post_run_coefficient_executor import (
    build_post_run_coefficient_executor_model,
    render_post_run_coefficient_executor_markdown,
    write_post_run_coefficient_executor_outputs,
)


pytestmark = pytest.mark.v1_5_formal_gate


def _write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    keys = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return path


def _seed_complete_post_run_evidence(root, *, devices=("001", "002")):
    _write_json(
        root / "formal_plan_snapshot.json",
        {
            "devices": {
                "gas_analyzers": [
                    {"runtime_device_id": device_id, "serial_port": f"COM{35 + index}"}
                    for index, device_id in enumerate(devices)
                ]
            }
        },
    )
    _write_json(root / "initialization_readiness.json", {"overall_status": "ready"})
    _write_json(root / "pressure_reference.json", {"reference_device": "COM22"})
    _write_json(root / "pressure_senco9_review.json", {"overall_status": "ready"})
    _write_csv(
        root / "temperature_current_point_review.csv",
        [{"analyzer_device_id": device_id, "status": "ready"} for device_id in devices],
    )
    _write_json(root / "v1_5_run_evidence_status.json", {"overall_status": "complete"})
    _write_json(root / "v1_5_formal_archive_closure_index.json", {"overall_status": "ready"})
    _write_json(
        root / "coefficient_database_sidecar.json",
        {
            "database_target_tables": ["coefficient_write_events", "audit_events"],
            "suggested_rows": [{"status": "written_readback_verified"}],
        },
    )
    _write_json(root / "main_senco_write_precheck_meta.json", {"overall_status": "ready"})
    _write_json(root / "post_write_reverification_review.json", {"overall_status": "ready"})

    candidate_rows = []
    model_rows = []
    trim_rows = []
    for component in ("co2", "h2o"):
        for device_id in devices:
            candidate_rows.append(
                {
                    "analyzer_device_id": device_id,
                    "component": component,
                    "candidate_status": "ready",
                    "sample_count": "12",
                }
            )
            model_rows.append(
                {
                    "analyzer_device_id": device_id,
                    "component": component,
                    "recommended_model": "true",
                    "model_name": "current_atmosphere_no_pressure",
                }
            )
            trim_rows.append(
                {
                    "analyzer_device_id": device_id,
                    "component": component,
                    "trim_status": "review_ready",
                }
            )

    candidate_dir = root / "candidate_coefficients"
    _write_csv(candidate_dir / "candidate_policy_summary.csv", candidate_rows)
    _write_csv(candidate_dir / "candidate_fit_residuals.csv", candidate_rows)
    model_dir = root / "model_selection"
    _write_csv(model_dir / "model_selection_summary.csv", model_rows)
    _write_csv(model_dir / "linear_trim_review.csv", trim_rows)


def test_post_run_executor_builds_ready_plan_without_touching_devices(tmp_path):
    run_dir = tmp_path / "run"
    _seed_complete_post_run_evidence(run_dir)

    model = build_post_run_coefficient_executor_model(run_dir=run_dir)
    stages = {row["stage_id"]: row for row in model["stages"]}
    devices = {row["device_id"]: row for row in model["devices"]}

    assert model["overall_status"] == "ready_for_next_automatic_step"
    assert model["physical_boundaries"]["offline_only"] is True
    assert model["physical_boundaries"]["opens_com_ports"] is False
    assert model["physical_boundaries"]["controls_valves_or_pace"] is False
    assert model["physical_boundaries"]["writes_coefficients"] is False
    assert model["workflow_contract"]["pressure_before_components"] is True
    assert model["workflow_contract"]["temperature_before_components"] is True
    assert model["workflow_contract"]["fit_all_eligible_stable_points"] is True
    assert model["workflow_contract"]["co2_zero_anchor_distinct_from_h2o_dry_anchor"] is True
    assert stages["pressure_input_quantity"]["status"] == "ready"
    assert stages["temperature_input_quantity"]["status"] == "ready"
    assert stages["component_candidate_fit"]["status"] == "ready"
    assert devices["001"]["overall_status"] == "ready_for_controlled_write_review"
    assert devices["002"]["co2_status"] == "candidate_ready_co2"
    assert devices["002"]["h2o_status"] == "candidate_ready_h2o"


def test_post_run_executor_blocks_missing_pressure_and_temperature_inputs(tmp_path):
    run_dir = tmp_path / "blocked"
    _write_json(
        run_dir / "formal_plan_snapshot.json",
        {"devices": {"gas_analyzers": [{"runtime_device_id": "079"}]}},
    )
    _write_json(run_dir / "initialization_readiness.json", {"overall_status": "ready"})
    candidate_dir = run_dir / "candidate_coefficients"
    _write_csv(
        candidate_dir / "candidate_policy_summary.csv",
        [
            {"analyzer_device_id": "079", "component": "co2", "candidate_status": "ready"},
            {"analyzer_device_id": "079", "component": "h2o", "candidate_status": "ready"},
        ],
    )
    _write_csv(candidate_dir / "candidate_fit_residuals.csv", [])
    model_dir = run_dir / "model_selection"
    _write_csv(
        model_dir / "model_selection_summary.csv",
        [
            {"analyzer_device_id": "079", "component": "co2", "recommended_model": "true"},
            {"analyzer_device_id": "079", "component": "h2o", "recommended_model": "true"},
        ],
    )

    model = build_post_run_coefficient_executor_model(run_dir=run_dir)
    stages = {row["stage_id"]: row for row in model["stages"]}
    device = model["devices"][0]

    assert model["overall_status"] == "blocked"
    assert stages["pressure_input_quantity"]["status"] == "blocked"
    assert stages["temperature_input_quantity"]["status"] == "blocked"
    assert device["overall_status"] == "blocked_or_partial"
    assert "needs_senco9_review_or_calibration" in device["blockers"]
    assert "needs_senco78_review_or_temperature_gate" in device["blockers"]


def test_post_run_executor_writes_machine_and_chinese_reviewer_outputs(tmp_path):
    run_dir = tmp_path / "run"
    output_dir = tmp_path / "post_run_executor"
    _seed_complete_post_run_evidence(run_dir, devices=("077",))

    model = build_post_run_coefficient_executor_model(run_dir=run_dir)
    paths = write_post_run_coefficient_executor_outputs(model, output_dir)
    manifest = json.loads(paths["manifest"].read_text(encoding="utf-8"))
    summary = paths["summary"].read_text(encoding="utf-8-sig")

    assert paths["manifest"].exists()
    assert paths["devices"].exists()
    assert paths["closure_gaps"].exists()
    assert paths["execution_plan"].exists()
    assert paths["controlled_write_package"].exists()
    assert paths["post_write_reverification_plan"].exists()
    assert paths["archive_gap_list"].exists()
    assert manifest["devices"][0]["device_id"] == "077"
    assert manifest["controlled_write_package"][0]["device_id"] == "077"
    assert manifest["post_write_reverification_plan"][0]["device_id"] == "077"
    assert "V1.5 校准后系数闭环执行计划" in summary
    assert "压力 P 和温度 T" in summary
    assert "采样窗口必须在气路/水路保持开放流通时取得" in summary

    with paths["controlled_write_package"].open(encoding="utf-8-sig", newline="") as handle:
        write_rows = list(csv.DictReader(handle))
    assert any(
        row["component"] == "co2_senco1_senco3"
        and row["requires_explicit_authorization"] == "true"
        and row["writes_senco"] == "reviewed_payload_only"
        for row in write_rows
    )

    with paths["post_write_reverification_plan"].open(encoding="utf-8-sig", newline="") as handle:
        reverify_rows = list(csv.DictReader(handle))
    assert any(
        row["component"] == "co2"
        and "gas route must remain open" in row["route_contract"]
        for row in reverify_rows
    )


def test_post_run_executor_cli_exports_no_write_manifest(tmp_path, capsys):
    run_dir = tmp_path / "cli_run"
    output_dir = tmp_path / "cli_out"
    _seed_complete_post_run_evidence(run_dir, devices=("084",))

    rc = export_executor_main(["--run-dir", str(run_dir), "--output-dir", str(output_dir)])
    cli_payload = json.loads(capsys.readouterr().out)
    manifest = json.loads((output_dir / "executor_manifest.json").read_text(encoding="utf-8"))
    summary = (output_dir / "executor_summary.md").read_text(encoding="utf-8-sig")

    assert rc == 0
    assert cli_payload["status"] == manifest["overall_status"]
    assert cli_payload["physical_boundaries"]["opens_com_ports"] is False
    assert manifest["physical_boundaries"]["writes_coefficients"] is False
    assert (output_dir / "controlled_write_package.csv").exists()
    assert (output_dir / "post_write_reverification_plan.csv").exists()
    assert "逐台设备状态" in summary


def test_post_run_executor_markdown_keeps_fit_and_verification_contract_visible(tmp_path):
    run_dir = tmp_path / "render_run"
    _seed_complete_post_run_evidence(run_dir, devices=("091",))

    model = build_post_run_coefficient_executor_model(run_dir=run_dir)
    markdown = render_post_run_coefficient_executor_markdown(model)

    assert "fit / verification 标签不默认排除样本" in markdown
    assert "CO2 零气低端锚点和 H2O 干气低水锚点物理意义不同" in markdown


def test_post_run_executor_discovers_r4_style_candidate_artifacts(tmp_path):
    run_dir = tmp_path / "r4_style"
    _write_json(
        run_dir / "formal_plan_snapshot.json",
        {"devices": {"gas_analyzers": [{"runtime_device_id": "077"}]}},
    )
    _write_json(run_dir / "initialization_archive_confirmation_20260611.json", {"status": "archived"})
    _write_json(run_dir / "pressure_senco9_review.json", {"overall_status": "ready"})
    _write_json(run_dir / "temperature_channel_review_summary.json", {"overall_status": "ready"})

    co2_dir = run_dir / "co2_best_writeable_candidate_search_20260610"
    _write_csv(
        co2_dir / "co2_recommended_writeable_summary.csv",
        [
            {
                "device_id": "077",
                "model_id": "relative_balanced_all_points",
                "recommended_model_id": "relative_balanced_all_points",
                "point_count": "45",
            }
        ],
    )
    h2o_dir = run_dir / "h2o_senco24_candidate_review_20260610_lowdry_formal_repair"
    _write_csv(
        h2o_dir / "h2o_senco24_device_policy.csv",
        [
            {
                "component": "h2o",
                "analyzer_device_id": "077",
                "candidate_status": "candidate_fit_review_required",
                "selected_model_terms": "intercept;R;R2;R3;T;T2;RT",
                "blocked_reasons": "",
            }
        ],
    )
    trim_dir = run_dir / "h2o_senco6_trim_after_wet_only_relfit_20260609"
    _write_csv(
        trim_dir / "h2o_senco6_linear_trim_candidate_summary.csv",
        [{"device_id": "077", "candidate_status": "review_ready", "payload_C0": "0.0", "payload_C1": "1.0"}],
    )

    model = build_post_run_coefficient_executor_model(run_dir=run_dir)
    stages = {row["stage_id"]: row for row in model["stages"]}
    device = model["devices"][0]

    assert stages["identity_initialization"]["status"] == "ready"
    assert stages["temperature_input_quantity"]["status"] == "ready"
    assert stages["component_candidate_fit"]["status"] == "ready"
    assert stages["output_layer_trim_review"]["status"] == "ready"
    assert device["co2_status"] == "candidate_ready_co2"
    assert device["h2o_status"] == "candidate_ready_h2o"
    assert device["overall_status"] == "ready_for_controlled_write_review"


def test_post_run_executor_uses_latest_candidate_review_instead_of_stale_blocker(tmp_path):
    run_dir = tmp_path / "latest_candidate"
    _write_json(
        run_dir / "formal_plan_snapshot.json",
        {"devices": {"gas_analyzers": [{"runtime_device_id": "091"}]}},
    )
    _write_json(run_dir / "initialization_archive_confirmation_20260611.json", {"status": "archived"})
    _write_json(run_dir / "pressure_senco9_review.json", {"overall_status": "ready"})
    _write_json(run_dir / "temperature_channel_review_summary.json", {"overall_status": "ready"})

    old_dir = run_dir / "candidate_fit_old_blocked"
    _write_csv(
        old_dir / "candidate_policy_summary.csv",
        [
            {
                "component": "co2",
                "analyzer_device_id": "091",
                "candidate_status": "blocked",
                "blocked_reasons": "formal_candidate_review_not_ready;fit_samples<10",
            },
            {
                "component": "h2o",
                "analyzer_device_id": "091",
                "candidate_status": "blocked",
                "blocked_reasons": "formal_candidate_review_not_ready;fit_samples<10",
            },
        ],
    )
    _write_csv(
        old_dir / "model_selection_summary.csv",
        [
            {"component": "co2", "analyzer_device_id": "091", "recommended_model": "true"},
            {"component": "h2o", "analyzer_device_id": "091", "recommended_model": "true"},
        ],
    )

    new_dir = run_dir / "candidate_fit_new_ready"
    _write_csv(
        new_dir / "candidate_policy_summary.csv",
        [
            {
                "component": "co2",
                "analyzer_device_id": "091",
                "candidate_status": "fit_ready_requires_verification",
                "blocked_reasons": "",
            },
            {
                "component": "h2o",
                "analyzer_device_id": "091",
                "candidate_status": "fit_ready_requires_verification",
                "blocked_reasons": "",
            },
        ],
    )
    _write_csv(
        new_dir / "model_selection_summary.csv",
        [
            {"component": "co2", "analyzer_device_id": "091", "recommended_model": "true"},
            {"component": "h2o", "analyzer_device_id": "091", "recommended_model": "true"},
        ],
    )
    trim_dir = run_dir / "linear_trim"
    _write_csv(
        trim_dir / "linear_trim_review.csv",
        [{"device_id": "091", "trim_status": "review_ready"}],
    )

    model = build_post_run_coefficient_executor_model(run_dir=run_dir)
    device = model["devices"][0]

    assert device["co2_status"] == "candidate_ready_co2"
    assert device["h2o_status"] == "candidate_ready_h2o"
    assert not any("formal_candidate_review_not_ready" in row["reason"] for row in model["closure_gaps"])


def test_post_run_executor_reports_partial_when_one_device_is_blocked(tmp_path):
    run_dir = tmp_path / "one_device_blocked"
    _seed_complete_post_run_evidence(run_dir, devices=("001", "002"))

    blocked_dir = run_dir / "candidate_fit_new_h2o_blocked"
    _write_csv(
        blocked_dir / "candidate_policy_summary.csv",
        [
            {
                "component": "h2o",
                "analyzer_device_id": "002",
                "candidate_status": "blocked",
                "blocked_reasons": "h2o_state_transfer_failed",
            }
        ],
    )

    model = build_post_run_coefficient_executor_model(run_dir=run_dir)
    devices = {row["device_id"]: row for row in model["devices"]}

    assert model["overall_status"] == "partial"
    assert devices["001"]["overall_status"] == "ready_for_controlled_write_review"
    assert devices["002"]["overall_status"] == "blocked_or_partial"
    assert "h2o_blocked:h2o_state_transfer_failed" in devices["002"]["blockers"]


def test_post_run_executor_blocks_device_root_cause_unqualified_even_when_candidates_ready(tmp_path):
    run_dir = tmp_path / "device_quality_blocked"
    _seed_complete_post_run_evidence(run_dir, devices=("079",))
    quality_dir = run_dir / "relative_error_root_cause"
    _write_csv(
        quality_dir / "id079_root_cause_flags.csv",
        [
            {
                "component": "co2",
                "device_id": "079",
                "max_abs_relative_error_pct": "47.511",
                "reason_short": "ratio_stable_but_curve_inconsistent_not_window_noise",
            }
        ],
    )

    model = build_post_run_coefficient_executor_model(run_dir=run_dir)
    device = model["devices"][0]

    assert model["overall_status"] == "partial"
    assert device["device_quality_status"].startswith("device_rejected_or_unqualified:")
    assert device["overall_status"] == "blocked_or_partial"
    assert any("device_rejected_or_unqualified" in blocker for blocker in device["blockers"])


def test_post_run_executor_does_not_accept_blocked_pressure_completion_csv(tmp_path):
    run_dir = tmp_path / "blocked_pressure_completion"
    _seed_complete_post_run_evidence(run_dir, devices=("079",))
    pressure_completion = _write_csv(
        run_dir / "pressure_channel_completion_summary.csv",
        [
            {
                "overall_status": "blocked",
                "device_count": "6",
                "ready_device_count": "1",
                "blocked_device_count": "5",
            }
        ],
    )

    model = build_post_run_coefficient_executor_model(
        run_dir=run_dir,
        pressure_review_json=pressure_completion,
    )
    stages = {row["stage_id"]: row for row in model["stages"]}
    device = model["devices"][0]

    assert model["overall_status"] == "partial"
    assert stages["pressure_input_quantity"]["status"] == "partial"
    assert stages["pressure_input_quantity"]["reason"] == "invalid_roles=pressure_review"
    assert device["pressure_status"] == "needs_senco9_review_or_calibration"


def test_post_run_executor_uses_per_device_pressure_completion_readiness(tmp_path):
    run_dir = tmp_path / "per_device_pressure_completion"
    _seed_complete_post_run_evidence(run_dir, devices=("091", "077"))
    pressure_completion = _write_csv(
        run_dir / "pressure_channel_completion_summary.csv",
        [
            {
                "overall_status": "blocked",
                "device_count": "2",
                "ready_device_count": "1",
                "blocked_device_count": "1",
            }
        ],
    )
    _write_csv(
        run_dir / "pressure_channel_device_readiness.csv",
        [
            {
                "analyzer_device_id": "091",
                "readiness_status": "pass",
                "readiness_reasons": "",
            },
            {
                "analyzer_device_id": "077",
                "readiness_status": "blocked",
                "readiness_reasons": "post_write_offset_out_of_limit",
            },
        ],
    )

    model = build_post_run_coefficient_executor_model(
        run_dir=run_dir,
        pressure_review_json=pressure_completion,
    )
    devices = {row["device_id"]: row for row in model["devices"]}

    assert model["overall_status"] == "partial"
    assert devices["091"]["pressure_status"] == "ready"
    assert devices["091"]["overall_status"] == "ready_for_controlled_write_review"
    assert devices["077"]["pressure_status"] == (
        "needs_senco9_review_or_calibration:post_write_offset_out_of_limit"
    )
    assert devices["077"]["overall_status"] == "blocked_or_partial"


def test_post_run_executor_accepts_initialization_ready_with_warnings(tmp_path):
    run_dir = tmp_path / "initialization_ready_with_warnings"
    _seed_complete_post_run_evidence(run_dir, devices=("001",))
    _write_json(
        run_dir / "initialization_readiness.json",
        {"readiness_status": "initialization_ready_with_warnings"},
    )

    model = build_post_run_coefficient_executor_model(run_dir=run_dir)
    stages = {row["stage_id"]: row for row in model["stages"]}

    assert stages["identity_initialization"]["status"] == "ready"


def test_post_run_executor_does_not_treat_failed_process_record_as_reverification(tmp_path):
    run_dir = tmp_path / "failed_reverify"
    _seed_complete_post_run_evidence(run_dir, devices=("001",))
    (run_dir / "post_write_reverification_review.json").unlink()
    _write_json(
        run_dir / "h2o_postwrite_reverify_process_record.json",
        {"returncode": 1, "stderr_tail": "FileNotFoundError: [WinError 206]"},
    )

    model = build_post_run_coefficient_executor_model(run_dir=run_dir)
    stages = {row["stage_id"]: row for row in model["stages"]}

    assert stages["post_write_reverification"]["status"] == "not_attempted"
    assert "h2o_postwrite_reverify_process_record.json" not in "\n".join(
        model["artifact_paths"]["post_write_reverification"]
    )


def test_post_run_executor_does_not_accept_failed_initialization_confirmation(tmp_path):
    run_dir = tmp_path / "failed_init"
    _write_json(
        run_dir / "formal_plan_snapshot.json",
        {"devices": {"gas_analyzers": [{"runtime_device_id": "077"}]}},
    )
    _write_json(run_dir / "initialization_archive_confirmation_20260611.json", {"status": "failed"})
    _write_json(run_dir / "pressure_senco9_review.json", {"overall_status": "ready"})
    _write_json(run_dir / "temperature_channel_review_summary.json", {"overall_status": "ready"})

    model = build_post_run_coefficient_executor_model(run_dir=run_dir)
    stages = {row["stage_id"]: row for row in model["stages"]}
    device = model["devices"][0]

    assert stages["identity_initialization"]["status"] == "partial"
    assert "invalid_roles=initialization_readiness" == stages["identity_initialization"]["reason"]
    assert device["identity_status"] == "missing_initial_identity_or_getco_snapshot"


def test_post_run_executor_accepts_getco_identity_snapshot_as_initialization_evidence(tmp_path):
    run_dir = tmp_path / "getco_identity_snapshot_init"
    _seed_complete_post_run_evidence(run_dir, devices=("077", "091"))
    (run_dir / "initialization_readiness.json").unlink()
    _write_csv(
        run_dir / "candidate_fit_all6_20260613" / "getco_snapshot_before_main_write_20260614" / "getco_component_snapshot_identity.csv",
        [
            {
                "analyzer_name": "ga01",
                "configured_device_id": "077",
                "analyzer_device_id": "077",
                "runtime_device_id": "077",
                "requested_groups": "1,2,3,4,5,6,7,8,9",
                "found_groups": "1,2,3,4,5,6,7,8,9",
                "all_groups_found": "True",
                "identity_before": "077",
                "identity_after": "077",
                "identity_verified": "True",
                "error": "",
                "writes_senco": "False",
                "writes_device_id": "False",
                "controls_water_or_gas_routes": "False",
                "controls_pace": "False",
            },
            {
                "analyzer_name": "ga02",
                "configured_device_id": "091",
                "analyzer_device_id": "091",
                "runtime_device_id": "091",
                "requested_groups": "1,2,3,4,5,6,7,8,9",
                "found_groups": "1,2,3,4,5,6,7,8,9",
                "all_groups_found": "True",
                "identity_before": "091",
                "identity_after": "091",
                "identity_verified": "True",
                "error": "",
                "writes_senco": "False",
                "writes_device_id": "False",
                "controls_water_or_gas_routes": "False",
                "controls_pace": "False",
            },
        ],
    )

    model = build_post_run_coefficient_executor_model(run_dir=run_dir)
    stages = {row["stage_id"]: row for row in model["stages"]}

    assert stages["identity_initialization"]["status"] == "ready"
    assert "getco_component_snapshot_identity.csv" in "\n".join(model["artifact_paths"]["initialization_readiness"])
    assert {row["identity_status"] for row in model["devices"]} == {"ready"}


def test_post_run_executor_rejects_incomplete_getco_identity_snapshot(tmp_path):
    run_dir = tmp_path / "incomplete_getco_identity_snapshot_init"
    _seed_complete_post_run_evidence(run_dir, devices=("077",))
    (run_dir / "initialization_readiness.json").unlink()
    _write_csv(
        run_dir / "candidate_fit_all6_20260613" / "getco_snapshot_before_main_write_20260614" / "getco_component_snapshot_identity.csv",
        [
            {
                "analyzer_name": "ga01",
                "configured_device_id": "077",
                "analyzer_device_id": "077",
                "runtime_device_id": "077",
                "requested_groups": "1,2,3,4,5,6,7,8,9",
                "found_groups": "1,2,3,4,5,6,7,8",
                "all_groups_found": "False",
                "identity_before": "077",
                "identity_after": "077",
                "identity_verified": "True",
                "error": "",
                "writes_senco": "False",
                "writes_device_id": "False",
                "controls_water_or_gas_routes": "False",
                "controls_pace": "False",
            }
        ],
    )

    model = build_post_run_coefficient_executor_model(run_dir=run_dir)
    stages = {row["stage_id"]: row for row in model["stages"]}
    device = model["devices"][0]

    assert stages["identity_initialization"]["status"] == "partial"
    assert stages["identity_initialization"]["reason"] == "invalid_roles=initialization_readiness"
    assert device["identity_status"] == "missing_initial_identity_or_getco_snapshot"


def test_post_run_executor_indexes_database_sidecar_but_still_requires_archive_closure(tmp_path):
    run_dir = tmp_path / "database_sidecar_only"
    _seed_complete_post_run_evidence(run_dir, devices=("001",))
    (run_dir / "v1_5_formal_archive_closure_index.json").unlink()

    model = build_post_run_coefficient_executor_model(run_dir=run_dir)
    stages = {row["stage_id"]: row for row in model["stages"]}

    assert model["artifact_paths"]["database_sidecar"]
    assert stages["archive_database_reports"]["status"] == "partial"
    assert stages["archive_database_reports"]["reason"] == "missing_roles=archive_closure"
