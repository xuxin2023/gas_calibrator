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
    _write_csv(
        root / "v1_5_fit_input_quality_summary.csv",
        [
            {
                "run_status": "pass",
                "fit_input_continuity_gate_status": "pass",
                "target_device_ids": ";".join(devices),
                "opens_com_ports": "False",
                "controls_water_or_gas_routes": "False",
                "writes_coefficients": "False",
            }
        ],
    )
    _write_csv(
        root / "v1_5_fit_input_quality_devices.csv",
        [
            {
                "component": component,
                "device_id": device_id,
                "fit_input_grade": "A",
                "fit_input_status": "usable_for_candidate_fit",
                "reject_reasons": "",
            }
            for device_id in devices
            for component in ("co2", "h2o")
        ],
    )

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
    assert model["workflow_contract"]["fit_input_quality_review_before_controlled_write"] is True
    assert model["workflow_contract"]["co2_zero_anchor_distinct_from_h2o_dry_anchor"] is True
    assert stages["pressure_input_quantity"]["status"] == "ready"
    assert stages["temperature_input_quantity"]["status"] == "ready"
    assert stages["component_candidate_fit"]["status"] == "ready"
    assert stages["fit_input_quality_review"]["status"] == "ready"
    assert devices["001"]["overall_status"] == "ready_for_controlled_write_review"
    assert devices["001"]["fit_input_quality_status"] == "ready"
    assert devices["002"]["co2_status"] == "candidate_ready_co2"
    assert devices["002"]["h2o_status"] == "candidate_ready_h2o"
    execution_order = {row["action"]: row for row in model["execution_order"]}
    assert execution_order["fit_input_quality_review"]["order"] < execution_order["co2_candidate_coefficients"]["order"]
    assert execution_order["fit_input_quality_review"]["order"] < execution_order["h2o_candidate_coefficients"]["order"]
    for action in ("co2_candidate_coefficients", "h2o_candidate_coefficients"):
        tool = execution_order[action]["tool"]
        assert "--require-fit-input-quality" in tool
        assert "--fit-input-quality-summary-csv" in tool
        assert str((run_dir / "v1_5_fit_input_quality_summary.csv").resolve()) in tool
        assert "--fit-input-quality-devices-csv" in tool
        assert str((run_dir / "v1_5_fit_input_quality_devices.csv").resolve()) in tool


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
    assert "压力输入量 P" in device["blocker_summary_zh"]
    assert "缺失证据" in device["next_action_zh"] or "阻断该设备" in device["next_action_zh"]
    assert any("压力输入量 P" in row["reason_zh"] for row in model["closure_gaps"])


def test_post_run_executor_blocks_controlled_write_when_fit_input_quality_missing(tmp_path):
    run_dir = tmp_path / "missing_fit_input_quality"
    _seed_complete_post_run_evidence(run_dir, devices=("077",))
    (run_dir / "v1_5_fit_input_quality_summary.csv").unlink()
    (run_dir / "v1_5_fit_input_quality_devices.csv").unlink()

    model = build_post_run_coefficient_executor_model(run_dir=run_dir)
    stages = {row["stage_id"]: row for row in model["stages"]}
    device = model["devices"][0]

    assert model["overall_status"] == "blocked"
    assert stages["fit_input_quality_review"]["status"] == "blocked"
    assert stages["fit_input_quality_review"]["reason"] == "missing_roles=fit_input_quality"
    assert device["fit_input_quality_status"] == "fit_input_quality_review_not_ready"
    assert device["overall_status"] == "blocked_or_partial"
    assert "fit_input_quality_review_not_ready" in device["blockers"]
    assert all(row["phase"] == "blocked" for row in model["controlled_write_package"])


def test_post_run_executor_blocks_only_device_rejected_by_fit_input_quality(tmp_path):
    run_dir = tmp_path / "fit_input_quality_rejects_one_device"
    _seed_complete_post_run_evidence(run_dir, devices=("077", "091"))
    _write_csv(
        run_dir / "v1_5_fit_input_quality_devices.csv",
        [
            {
                "component": "co2",
                "device_id": "077",
                "fit_input_grade": "A",
                "fit_input_status": "usable_for_candidate_fit",
                "reject_reasons": "",
            },
            {
                "component": "h2o",
                "device_id": "077",
                "fit_input_grade": "A",
                "fit_input_status": "usable_for_candidate_fit",
                "reject_reasons": "",
            },
            {
                "component": "co2",
                "device_id": "091",
                "fit_input_grade": "REJECT",
                "fit_input_status": "excluded_from_candidate_fit",
                "reject_reasons": "fit_input_continuity_gate_not_ready:segmented_route_evidence",
            },
            {
                "component": "h2o",
                "device_id": "091",
                "fit_input_grade": "A",
                "fit_input_status": "usable_for_candidate_fit",
                "reject_reasons": "",
            },
        ],
    )

    model = build_post_run_coefficient_executor_model(run_dir=run_dir)
    devices = {row["device_id"]: row for row in model["devices"]}
    ready_write_rows = [
        row
        for row in model["controlled_write_package"]
        if row["plan_status"] == "ready_for_controlled_write_review"
    ]

    assert model["overall_status"] == "partial"
    assert devices["077"]["overall_status"] == "ready_for_controlled_write_review"
    assert devices["091"]["overall_status"] == "blocked_or_partial"
    assert devices["091"]["fit_input_quality_status"].startswith("fit_input_quality_rejected:co2:")
    assert "fit_input_continuity_gate_not_ready" in devices["091"]["fit_input_quality_status"]
    assert {row["device_id"] for row in ready_write_rows} == {"077"}


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
    assert "V1.5 采集后系数闭环执行计划" in summary
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
    with paths["devices"].open(encoding="utf-8-sig", newline="") as handle:
        device_rows = list(csv.DictReader(handle))
    assert device_rows[0]["blocker_summary_zh"] == "无阻断项，设备可进入受控写入评审。"


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


def test_post_run_executor_cli_accepts_explicit_co2_source_state_gate(tmp_path, capsys):
    run_dir = tmp_path / "cli_run_external_gate"
    output_dir = tmp_path / "cli_out_external_gate"
    gate_dir = tmp_path / "external_co2_source_state_gate"
    _seed_complete_post_run_evidence(run_dir, devices=("084",))
    _write_csv(
        gate_dir / "co2_s13_source_state_run_summary.csv",
        [
            {
                "write_gate_status": "blocked_source_state_discontinuity",
                "write_gate_blocker_count": "1",
                "write_gate_blocker_topics": "external_source_state_gate",
                "candidate_write_allowed": "False",
                "writes_coefficients": "False",
            }
        ],
    )

    rc = export_executor_main(
        [
            "--run-dir",
            str(run_dir),
            "--output-dir",
            str(output_dir),
            "--co2-source-state-gate",
            str(gate_dir),
        ]
    )
    cli_payload = json.loads(capsys.readouterr().out)
    manifest = json.loads((output_dir / "executor_manifest.json").read_text(encoding="utf-8"))
    stages = {row["stage_id"]: row for row in manifest["stages"]}

    assert rc == 0
    assert cli_payload["status"] == "blocked"
    assert stages["co2_source_state_write_gate"]["status"] == "blocked"
    assert str(gate_dir.resolve()) in manifest["artifact_paths"]["co2_source_state_gate"]


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
    _write_csv(
        run_dir / "v1_5_fit_input_quality_summary.csv",
        [
            {
                "run_status": "pass",
                "fit_input_continuity_gate_status": "pass",
                "opens_com_ports": "False",
                "controls_water_or_gas_routes": "False",
                "writes_coefficients": "False",
            }
        ],
    )
    _write_csv(
        run_dir / "v1_5_fit_input_quality_devices.csv",
        [
            {
                "component": component,
                "device_id": "077",
                "fit_input_grade": "A",
                "fit_input_status": "usable_for_candidate_fit",
                "reject_reasons": "",
            }
            for component in ("co2", "h2o")
        ],
    )

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


def test_post_run_executor_indexes_h2o_dry_anchor_bridge_review(tmp_path):
    run_dir = tmp_path / "dry_anchor_bridge_ready"
    _seed_complete_post_run_evidence(run_dir, devices=("022",))
    bridge_dir = run_dir / "h2o_dry_anchor_bridge_review_20260618"
    _write_json(
        bridge_dir / "h2o_dry_anchor_bridge_manifest.json",
        {
            "no_write": True,
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
            "dry_anchor_bridge_contract": (
                "Gas-route dry anchors use dewpoint/pressure-derived H2O targets "
                "before being allowed into any main fit."
            ),
        },
    )
    _write_csv(
        bridge_dir / "h2o_dry_anchor_bridge_device_summary.csv",
        [
            {
                "component": "h2o",
                "analyzer_device_id": "022",
                "dry_anchor_count": "3",
                "dry_anchor_compatible_count": "3",
                "dry_bridge_max_abs_error_mmol": "0.041",
                "dry_bridge_max_abs_relative_error_pct": "0.52",
                "recommendation": "dry_anchors_can_enter_low_end_fit_review",
            }
        ],
    )
    _write_csv(
        bridge_dir / "h2o_dry_anchor_bridge_predictions.csv",
        [
            {
                "component": "h2o",
                "analyzer_device_id": "022",
                "reference_dewpoint_c": "-32.0",
                "reference_pressure_hpa": "1012.0",
                "bridge_status": "bridge_fit_compatible",
            }
        ],
    )
    _write_csv(
        bridge_dir / "h2o_dry_anchor_bridge_strategy_comparison.csv",
        [{"strategy_id": "dry_le_0_relative", "worst_device_id": "022"}],
    )

    model = build_post_run_coefficient_executor_model(run_dir=run_dir)
    stages = {row["stage_id"]: row for row in model["stages"]}
    device = model["devices"][0]
    command_actions = {row["action"] for row in model["execution_order"]}

    assert stages["h2o_dry_anchor_bridge_review"]["status"] == "ready"
    assert "h2o_dry_anchor_bridge_review" in command_actions
    assert model["workflow_contract"]["h2o_dry_anchor_requires_dewpoint_pressure_temperature_bridge"] is True
    assert device["h2o_dry_anchor_bridge_status"] == "ready:dry_anchors_can_enter_low_end_fit_review"
    assert device["overall_status"] == "ready_for_controlled_write_review"
    assert model["artifact_paths"]["h2o_dry_anchor_bridge"]

    h2o_write = next(
        row
        for row in model["controlled_write_package"]
        if row["device_id"] == "022" and row["component"] == "h2o_senco2_senco4"
    )
    assert "dry-gas low-water anchors" in h2o_write["physical_gate"]
    assert "dry_anchors_can_enter_low_end_fit_review" in h2o_write["source_status"]


def test_post_run_executor_blocks_device_when_h2o_dry_anchor_bridge_requires_new_evidence(tmp_path):
    run_dir = tmp_path / "dry_anchor_bridge_blocked"
    _seed_complete_post_run_evidence(run_dir, devices=("022", "030"))
    bridge_dir = run_dir / "h2o_dry_anchor_bridge_review_20260618"
    _write_json(
        bridge_dir / "h2o_dry_anchor_bridge_manifest.json",
        {"no_write": True, "opens_com_ports": False, "writes_coefficients": False},
    )
    _write_csv(
        bridge_dir / "h2o_dry_anchor_bridge_device_summary.csv",
        [
            {
                "component": "h2o",
                "analyzer_device_id": "022",
                "dry_anchor_count": "2",
                "dry_anchor_compatible_count": "0",
                "dry_bridge_max_abs_error_mmol": "0.83",
                "dry_bridge_max_abs_relative_error_pct": "14.2",
                "recommendation": "collect_new_formal_dry_h2o_anchor_evidence",
            },
            {
                "component": "h2o",
                "analyzer_device_id": "030",
                "dry_anchor_count": "2",
                "dry_anchor_compatible_count": "2",
                "dry_bridge_max_abs_error_mmol": "0.03",
                "dry_bridge_max_abs_relative_error_pct": "0.4",
                "recommendation": "dry_anchors_can_enter_low_end_fit_review",
            },
        ],
    )

    model = build_post_run_coefficient_executor_model(run_dir=run_dir)
    devices = {row["device_id"]: row for row in model["devices"]}

    assert model["overall_status"] == "partial"
    assert devices["030"]["overall_status"] == "ready_for_controlled_write_review"
    assert devices["022"]["overall_status"] == "blocked_or_partial"
    assert (
        "h2o_dry_anchor_bridge_blocked:collect_new_formal_dry_h2o_anchor_evidence"
        in devices["022"]["blockers"]
    )
    assert any("h2o_dry_anchor_bridge_blocked" in row["reason"] for row in model["closure_gaps"])


def test_post_run_executor_blocks_co2_write_when_source_state_gate_blocks(tmp_path):
    run_dir = tmp_path / "co2_source_state_blocked"
    _seed_complete_post_run_evidence(run_dir, devices=("077", "084"))
    gate_dir = run_dir / "co2_s13_source_state_discontinuity_audit_20260621"
    _write_csv(
        gate_dir / "co2_s13_source_state_run_summary.csv",
        [
            {
                "write_gate_status": "blocked_source_state_discontinuity",
                "write_gate_blocker_count": "2",
                "write_gate_blocker_topics": "mixed_source_temperature_group;non_affine_sawtooth_bias",
                "candidate_write_allowed": "False",
                "writes_coefficients": "False",
            }
        ],
    )

    model = build_post_run_coefficient_executor_model(run_dir=run_dir)
    stages = {row["stage_id"]: row for row in model["stages"]}
    devices = {row["device_id"]: row for row in model["devices"]}
    co2_write_rows = [
        row
        for row in model["controlled_write_package"]
        if row["component"] == "co2_senco1_senco3"
    ]

    assert model["overall_status"] == "blocked"
    assert stages["co2_source_state_write_gate"]["status"] == "blocked"
    assert "mixed_source_temperature_group" in stages["co2_source_state_write_gate"]["reason"]
    assert devices["077"]["overall_status"] == "blocked_or_partial"
    assert devices["077"]["co2_status"].startswith("co2_blocked:")
    assert any("co2_source_state_blocked" in blocker for blocker in devices["084"]["blockers"])
    assert co2_write_rows == []
    assert all(row["phase"] == "blocked" for row in model["controlled_write_package"])


def test_post_run_executor_blocks_co2_write_with_external_source_state_gate_file(tmp_path):
    run_dir = tmp_path / "co2_source_state_external_blocked"
    gate_dir = tmp_path / "external_co2_gate_file"
    _seed_complete_post_run_evidence(run_dir, devices=("077",))
    gate_file = _write_csv(
        gate_dir / "co2_s13_source_state_run_summary.csv",
        [
            {
                "write_gate_status": "blocked_source_state_discontinuity",
                "write_gate_blocker_count": "1",
                "write_gate_blocker_topics": "external_gate_file",
                "candidate_write_allowed": "False",
                "writes_coefficients": "False",
            }
        ],
    )

    model = build_post_run_coefficient_executor_model(
        run_dir=run_dir,
        co2_source_state_gate=gate_file,
    )
    stages = {row["stage_id"]: row for row in model["stages"]}

    assert model["overall_status"] == "blocked"
    assert stages["co2_source_state_write_gate"]["status"] == "blocked"
    assert model["artifact_paths"]["co2_source_state_gate"] == [str(gate_dir.resolve())]
    assert not [
        row
        for row in model["controlled_write_package"]
        if row["component"] == "co2_senco1_senco3"
    ]


def test_post_run_executor_allows_review_required_source_state_gate_without_blockers(tmp_path):
    run_dir = tmp_path / "co2_source_state_review_ready"
    _seed_complete_post_run_evidence(run_dir, devices=("077",))
    gate_dir = run_dir / "co2_s13_source_state_discontinuity_audit_20260621"
    _write_csv(
        gate_dir / "co2_s13_source_state_run_summary.csv",
        [
            {
                "write_gate_status": "review_required",
                "write_gate_blocker_count": "0",
                "write_gate_blocker_topics": "",
                "candidate_write_allowed": "True",
                "writes_coefficients": "False",
            }
        ],
    )

    model = build_post_run_coefficient_executor_model(run_dir=run_dir)
    stages = {row["stage_id"]: row for row in model["stages"]}
    device = model["devices"][0]

    assert stages["co2_source_state_write_gate"]["status"] == "ready"
    assert device["co2_status"] == "candidate_ready_co2"
    assert device["overall_status"] == "ready_for_controlled_write_review"
    assert any(
        row["device_id"] == "077" and row["component"] == "co2_senco1_senco3"
        for row in model["controlled_write_package"]
    )


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


def test_post_run_executor_uses_explicit_pressure_completion_alongside_review_json(tmp_path):
    run_dir = tmp_path / "explicit_pressure_completion"
    _seed_complete_post_run_evidence(run_dir, devices=("097", "091"))
    pressure_review = run_dir / "pressure_senco9_review.json"
    pressure_completion = _write_csv(
        run_dir / "pressure_channel_completion" / "pressure_channel_completion_summary.csv",
        [
            {
                "overall_status": "blocked",
                "device_count": "2",
                "ready_device_count": "1",
                "blocked_device_count": "1",
            }
        ],
    )
    pressure_readiness = _write_csv(
        run_dir / "pressure_channel_completion" / "pressure_channel_device_readiness.csv",
        [
            {
                "analyzer_device_id": "097",
                "readiness_status": "pass",
                "readiness_reasons": "",
            },
            {
                "analyzer_device_id": "091",
                "readiness_status": "blocked",
                "readiness_reasons": "needs_reverify_after_restart",
            },
        ],
    )

    model = build_post_run_coefficient_executor_model(
        run_dir=run_dir,
        pressure_review_json=pressure_review,
        pressure_completion_summary_csv=pressure_completion,
        pressure_device_readiness_csv=pressure_readiness,
    )
    devices = {row["device_id"]: row for row in model["devices"]}

    assert model["overall_status"] == "partial"
    assert devices["097"]["pressure_status"] == "ready"
    assert devices["097"]["overall_status"] == "ready_for_controlled_write_review"
    assert devices["091"]["pressure_status"] == "needs_senco9_review_or_calibration:needs_reverify_after_restart"
    assert devices["091"]["overall_status"] == "blocked_or_partial"


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
