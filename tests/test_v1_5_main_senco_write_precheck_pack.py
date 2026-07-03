import csv
import json

from gas_calibrator.tools.export_v1_5_main_senco_write_precheck_pack import (
    build_precheck_pack,
    main as precheck_cli,
)


def _write_csv(path, rows):
    keys = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path):
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _model_selection_dir(tmp_path, component):
    root = tmp_path / f"{component}_models"
    rows = [
        {
            "component": component,
            "analyzer_prefix": "ga02",
            "analyzer_device_id": "091",
            "model_name": "cubic_R_T_T2_RT",
            "model_terms": "intercept;R;R2;R3;T;T2;RT",
            "fit_status": "ok",
            "recommended_model": "true",
            "review_note": "main_model_candidate_for_senco1234_review",
            "factory_signal_health_gate": "pass",
            "max_abs_error": "1.2",
            "max_abs_relative_error_pct": "2.5",
            "coef_intercept": "1",
            "coef_R": "2",
            "coef_R2": "3",
            "coef_R3": "4",
            "coef_T": "5",
            "coef_T2": "6",
            "coef_RT": "7",
        },
        {
            "component": component,
            "analyzer_prefix": "ga01",
            "analyzer_device_id": "079",
            "model_name": "cubic_R_T_T2_RT",
            "model_terms": "intercept;R;R2;R3;T;T2;RT",
            "fit_status": "ok",
            "recommended_model": "true",
            "review_note": "blocked_by_factory_signal_health:block_optical_reference_health_review",
            "factory_signal_health_gate": "block_optical_reference_health_review",
            "max_abs_error": "101.5",
            "max_abs_relative_error_pct": "47.0",
            "coef_intercept": "1",
            "coef_R": "2",
            "coef_R2": "3",
            "coef_R3": "4",
            "coef_T": "5",
            "coef_T2": "6",
            "coef_RT": "7",
        },
    ]
    _write_csv(root / "model_selection_summary.csv", rows)
    return root


def _candidate_dir(tmp_path, component):
    root = tmp_path / f"{component}_candidate"
    _write_csv(
        root / "candidate_policy_summary.csv",
        [
            {
                "component": component,
                "analyzer_prefix": "ga02",
                "analyzer_device_id": "091",
                "candidate_status": "fit_ready_requires_verification",
                "allowed_to_fit": "True",
                "allowed_for_review": "False",
                "blocked_reasons": "",
                "warning_reasons": "",
            },
            {
                "component": component,
                "analyzer_prefix": "ga01",
                "analyzer_device_id": "079",
                "candidate_status": "blocked",
                "allowed_to_fit": "False",
                "allowed_for_review": "False",
                "blocked_reasons": "factory_signal_health_block",
                "warning_reasons": "",
            },
        ],
    )
    return root


def _plan_path(tmp_path):
    plan = {
        "devices": {
            "gas_analyzers": [
                {
                    "name": "ga02",
                    "port": "COM36",
                    "device_id": "091",
                    "runtime_device_id": "091",
                    "configured_device_id": "030",
                    "runtime_identity_bound": True,
                    "identity_binding_source": "v1_5_getco_component_snapshot",
                    "identity_binding_frozen": True,
                },
                {
                    "name": "ga01",
                    "port": "COM35",
                    "device_id": "079",
                    "runtime_device_id": "079",
                    "configured_device_id": "023",
                    "runtime_identity_bound": True,
                    "identity_binding_source": "v1_5_getco_component_snapshot",
                    "identity_binding_frozen": True,
                },
            ]
        }
    }
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(plan), encoding="utf-8")
    return path


def _snapshot_path(tmp_path):
    snapshot = {
        "091": {
            "GETCO1_before": [1, 2, 3, 4, 0, 0],
            "GETCO3_before": [5, 6, 7, 0, 0, 0],
            "GETCO5_before": [0, 1],
            "GETCO2_before": [1, 2, 3, 4, 0, 0],
            "GETCO4_before": [5, 6, 7, 0, 0, 0],
            "GETCO6_before": [0, 1],
        }
    }
    path = tmp_path / "old_getco.json"
    path.write_text(json.dumps(snapshot), encoding="utf-8")
    return path


def test_precheck_pack_generates_component_specific_main_commands(tmp_path):
    out = tmp_path / "out"

    paths = build_precheck_pack(
        co2_model_selection_dir=_model_selection_dir(tmp_path, "co2"),
        h2o_model_selection_dir=_model_selection_dir(tmp_path, "h2o"),
        co2_candidate_dir=_candidate_dir(tmp_path, "co2"),
        h2o_candidate_dir=_candidate_dir(tmp_path, "h2o"),
        output_dir=out,
        plan_path=_plan_path(tmp_path),
        old_coefficients_path=_snapshot_path(tmp_path),
        include_devices=("091",),
    )

    commands = _read_csv(paths["commands"])
    ready_commands = [row for row in commands if row["analyzer_device_id"] == "091"]
    assert {row["senco_channel"] for row in ready_commands} == {"SENCO1", "SENCO2", "SENCO3", "SENCO4"}
    assert any(row["command"].startswith("SENCO1,YGAS,FFF,1.00000e00,2.00000e00") for row in ready_commands)
    assert any(row["command"].startswith("SENCO3,YGAS,FFF,5.00000e00,6.00000e00,7.00000e00") for row in ready_commands)
    assert any(row["command"].startswith("SENCO2,YGAS,FFF,1.00000e00,2.00000e00") for row in ready_commands)
    assert any(row["command"].startswith("SENCO4,YGAS,FFF,5.00000e00,6.00000e00,7.00000e00") for row in ready_commands)
    assert all(row["command"].endswith(",0.00000e00,0.00000e00,0.00000e00") for row in ready_commands if row["senco_channel"] in {"SENCO3", "SENCO4"})

    summary = {row["analyzer_device_id"]: row for row in _read_csv(paths["summary"])}
    assert summary["091"]["overall_status"] == "model_ready_for_main_senco_review"
    assert summary["079"]["overall_status"] == "blocked_before_write_review"

    neutral = _read_csv(paths["neutral"])
    assert any(row["analyzer_device_id"] == "091" and row["linear_senco_channel"] == "SENCO5" for row in neutral)
    assert any(row["analyzer_device_id"] == "091" and row["linear_senco_channel"] == "SENCO6" for row in neutral)


def test_precheck_pack_blocks_actual_write_when_old_snapshot_missing(tmp_path):
    paths = build_precheck_pack(
        co2_model_selection_dir=_model_selection_dir(tmp_path, "co2"),
        h2o_model_selection_dir=_model_selection_dir(tmp_path, "h2o"),
        co2_candidate_dir=_candidate_dir(tmp_path, "co2"),
        h2o_candidate_dir=_candidate_dir(tmp_path, "h2o"),
        output_dir=tmp_path / "out_missing_snapshot",
        plan_path=_plan_path(tmp_path),
        include_devices=("091",),
    )

    summary = {row["analyzer_device_id"]: row for row in _read_csv(paths["summary"])}
    assert summary["091"]["overall_status"] == "model_ready_for_main_senco_review"
    assert summary["091"]["co2_model_blockers"] == ""
    assert summary["091"]["h2o_model_blockers"] == ""
    commands = [row for row in _read_csv(paths["commands"]) if row["analyzer_device_id"] == "091"]
    assert commands
    assert all(row["status"] == "pending_not_executed_requires_snapshot_or_prerequisite" for row in commands)
    assert all("old_getco_snapshot_not_bound" in row["write_gate_blockers"] for row in commands)


def test_precheck_pack_cli_writes_meta_and_chinese_report(tmp_path):
    output_dir = tmp_path / "cli_out"
    result = precheck_cli(
        [
            "--co2-model-selection-dir",
            str(_model_selection_dir(tmp_path, "co2")),
            "--h2o-model-selection-dir",
            str(_model_selection_dir(tmp_path, "h2o")),
            "--co2-candidate-dir",
            str(_candidate_dir(tmp_path, "co2")),
            "--h2o-candidate-dir",
            str(_candidate_dir(tmp_path, "h2o")),
            "--plan-json",
            str(_plan_path(tmp_path)),
            "--old-coefficients-json",
            str(_snapshot_path(tmp_path)),
            "--output-dir",
            str(output_dir),
            "--include-device-id",
            "091",
        ]
    )

    assert result == 0
    meta = json.loads((output_dir / "main_senco_write_precheck_meta.json").read_text(encoding="utf-8"))
    assert meta["no_write"] is True
    assert meta["opens_com"] is False
    assert meta["writes_senco"] is False
    report = (output_dir / "main_senco_write_precheck_pack_zh.md").read_text(encoding="utf-8")
    assert "V1.5 主系数写入前评审包" in report
