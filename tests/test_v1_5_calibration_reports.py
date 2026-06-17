import csv
import json
import zipfile
from pathlib import Path

from gas_calibrator.tools.export_v1_5_calibration_reports import main as report_main
from gas_calibrator.validation.formal_contracts import RELEASED_UNCERTAINTY_INPUTS_TEMPLATE
from gas_calibrator.validation.formal_evidence_run import (
    prepare_formal_evidence_run,
    run_formal_evidence_sidecar,
)
from gas_calibrator.validation.formal_reports import (
    build_report_model_from_bundle,
    render_markdown,
    build_run_report,
    build_technical_report,
    build_formal_calibration_report,
    write_v1_5_calibration_reports,
)
from gas_calibrator.validation.pressure_channel import write_pressure_quick_check_csv


def _standard_gases():
    return [
        {
            "component": "co2",
            "cylinder_id": "CO2-900",
            "certificate_value": 900.0,
            "certificate_uncertainty": 0.9,
            "valid_until": "2027-01-01",
            "supplier": "standard-lab",
            "certificate_hash": "co2-cert-hash",
        },
        {
            "component": "h2o",
            "cylinder_id": "H2O-GEN-001",
            "certificate_value": 0.5,
            "certificate_uncertainty": 0.01,
            "valid_until": "2027-01-01",
            "supplier": "standard-lab",
            "certificate_hash": "h2o-cert-hash",
        },
    ]


def _pressure_reference():
    return {
        "device_id": "COM22-DPG-001",
        "certificate_id": "P-CERT-001",
        "certificate_uncertainty": 0.15,
        "valid_until": "2027-01-01",
        "certificate_hash": "pressure-cert-hash",
        "unit": "hPa",
    }


def _safe_config():
    return {
        "workflow": {
            "controlled_write": False,
            "postrun_corrected_delivery": {"enabled": False, "write_devices": False},
        },
        "validation": {
            "dry_collect": {"write_coefficients": False},
            "coefficient_roundtrip": {"write_back_same": False, "allow_write_modified": False},
        },
        "sencos": {},
    }


def _row(index: int, component: str):
    return {
        "sample_index": index,
        "sample_ts": f"2026-05-24T12:00:{index:02d}",
        "point_phase": component,
        "route": component,
        "pressure_mode": "ambient_open",
        "pressure_gauge_hpa": 1000.5 + index * 0.002,
        "controller_pressure": 1000.6 + index * 0.002,
        "pressure_atmosphere_hold_status": "verified",
        "pressure_atmosphere_hold_active": "true",
        "pressure_atmosphere_hold_strategy": "legacy_hold_thread",
        "dewpoint_c": -30.0 + index * 0.001,
        "ga01_frame_usable": "true",
        "ga01_mode2_contract_status": "pass",
        "ga01_mode2_qc_status": "pass",
        "ga01_mode2_tokens_json": json.dumps(
            ["YGAS", "001", "0900.000", "00.500", "1768.000", "00.410"],
            separators=(",", ":"),
        ),
        "ga01_raw": "YGAS,001,...",
        "ga01_ref_signal": 3322.0,
        "ga01_co2_signal": 4356.0,
        "ga01_h2o_signal": 2631.0,
        "ga01_chamber_temp_c": 25.0 + index * 0.001,
        "ga01_case_temp_c": 25.5,
        "ga01_pressure_kpa": 100.05 + index * 0.0002,
        "ga01_co2_ratio_f": 1.3000 + index * 0.0001,
        "ga01_co2_ppm": 900.0 + index * 0.01,
        "ga01_h2o_ratio_f": 0.7000 + index * 0.00001,
        "ga01_h2o_mmol": 0.5 + index * 0.0001,
    }


def _write_csv(path, rows):
    keys = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def _write_post_write_reverification_artifacts(run_dir):
    review_dir = run_dir / "post_write_reverification"
    review_dir.mkdir()
    _write_csv(
        review_dir / "post_write_reverification_device_summary.csv",
        [
            {
                "device_id": "100",
                "component": "co2",
                "point_count": 2,
                "pass_count": 2,
                "fail_count": 0,
                "not_evaluated_count": 0,
                "max_abs_error": 1.2,
                "max_abs_error_pct": 0.13,
                "status": "pass",
            }
        ],
    )
    _write_csv(
        review_dir / "post_write_reverification_points.csv",
        [
            {
                "device_id": "100",
                "component": "co2",
                "point_id": "post_write_900ppm",
                "standard_value": 900.0,
                "measured_value": 901.2,
                "unit": "ppm",
                "error": 1.2,
                "error_pct": 0.13,
                "limit_value": 1.5,
                "limit_basis": "co2_relative_pct",
                "status": "pass",
                "reason": "",
            }
        ],
    )
    (review_dir / "post_write_reverification_review.json").write_text(
        json.dumps(
            {
                "schema": "v1_5_post_write_reverification_review_v1",
                "created_at": "2026-05-24T12:30:00",
                "overall_status": "pass",
                "limits": {"co2_relative_pct": 1.5, "h2o_relative_pct": 2.0},
                "warnings": [],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    (review_dir / "post_write_reverification_review.md").write_text(
        "# V1.5 post-write reverification\n\noverall_status: pass\n",
        encoding="utf-8",
    )


def _make_evidence_bundle(tmp_path, *, quick_check=True, post_write=False):
    config_path = tmp_path / "runtime_config.json"
    config_path.write_text(json.dumps(_safe_config(), ensure_ascii=False), encoding="utf-8")
    gases_path = tmp_path / "standard_gases.json"
    gases_path.write_text(json.dumps({"standard_gases": _standard_gases()}, ensure_ascii=False), encoding="utf-8")
    reference_path = tmp_path / "source_pressure_reference.json"
    reference_path.write_text(json.dumps(_pressure_reference(), ensure_ascii=False), encoding="utf-8")
    prepared = prepare_formal_evidence_run(
        output_dir=tmp_path / "formal_evidence",
        operator="operator-a",
        analyzer_id="GA-001",
        run_id="v1_5_formal_demo",
        config_path=config_path,
        standard_gases_json=gases_path,
        pressure_reference_json=reference_path,
        lab="lab-a",
    )
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    rows = [_row(i, "co2") for i in range(1, 11)] + [_row(i, "h2o") for i in range(11, 21)]
    _write_csv(run_dir / "samples_20260524.csv", rows)
    if quick_check:
        write_pressure_quick_check_csv(run_dir, rows[:10], run_id="20260524")
    if post_write:
        _write_post_write_reverification_artifacts(run_dir)
    run_formal_evidence_sidecar(
        run_dir=run_dir,
        plan_path=prepared["plan"],
        pressure_reference_path=prepared["pressure_reference"],
        config_path=config_path,
        today="2026-05-24",
    )
    return run_dir / "formal_evidence_sidecar" / "evidence_bundle.json"


def _released_uncertainty_payload():
    inputs = []
    for component in ("CO2", "H2O"):
        inputs.extend(
            [
                {
                    "component": component,
                    "input_quantity": "standard_gas_certificate",
                    "distribution": "normal",
                    "standard_uncertainty": 0.9 if component == "CO2" else 0.01,
                    "sensitivity_coefficient": 1.0,
                    "status": "released",
                    "evidence_source": f"{component} certificate",
                },
                {
                    "component": component,
                    "input_quantity": "repeatability",
                    "distribution": "normal",
                    "standard_uncertainty": 0.02,
                    "sensitivity_coefficient": 1.0,
                    "status": "released",
                    "evidence_source": "A-grade sample statistics",
                },
                {
                    "component": component,
                    "input_quantity": "fit_residual",
                    "distribution": "normal",
                    "standard_uncertainty": 0.03,
                    "sensitivity_coefficient": 1.0,
                    "status": "released",
                    "evidence_source": "released fit residual budget",
                },
                {
                    "component": component,
                    "input_quantity": "analyzer_resolution",
                    "distribution": "rectangular",
                    "standard_uncertainty": 0.01,
                    "sensitivity_coefficient": 1.0,
                    "status": "released",
                    "evidence_source": "resolution budget",
                },
            ]
        )
    inputs.extend(
        [
            {
                "component": "CO2/H2O",
                "input_quantity": "pressure_channel_bias",
                "distribution": "normal",
                "standard_uncertainty": 0.02,
                "sensitivity_coefficient": 1.0,
                "status": "released",
                "evidence_source": "COM22 quick check",
            },
            {
                "component": "CO2/H2O",
                "input_quantity": "temperature_effect",
                "distribution": "normal",
                "standard_uncertainty": 0.01,
                "sensitivity_coefficient": 1.0,
                "status": "released",
                "evidence_source": "temperature sensitivity budget",
            },
            {
                "component": "CO2/H2O",
                "input_quantity": "sampling_stability",
                "distribution": "qualitative_gate",
                "standard_uncertainty": 0.0,
                "sensitivity_coefficient": 1.0,
                "status": "released",
                "evidence_source": "QC gate pass",
            },
            {
                "component": "H2O",
                "input_quantity": "dewpoint_or_humidity_reference",
                "distribution": "normal",
                "standard_uncertainty": 0.02,
                "sensitivity_coefficient": 1.0,
                "status": "released",
                "evidence_source": "humidity reference budget",
            },
            {
                "component": "H2O",
                "input_quantity": "water_vapor_correction",
                "distribution": "normal",
                "standard_uncertainty": 0.02,
                "sensitivity_coefficient": 1.0,
                "status": "released",
                "evidence_source": "water vapor correction budget",
            },
        ]
    )
    return {"released": True, "coverage_factor": 2.0, "release_basis": "unit-test released budget", "inputs": inputs}


def _write_uncertainty_json(tmp_path):
    path = tmp_path / "uncertainty_inputs.json"
    path.write_text(json.dumps(_released_uncertainty_payload(), ensure_ascii=False), encoding="utf-8")
    return path


def _assert_no_mojibake(text):
    bad_markers = ("锛", "姘", "灏", "鎶", "鍐", "璇", "€", "�")
    assert not any(marker in text for marker in bad_markers)


def test_report_model_preserves_scope_boundaries_and_no_write(tmp_path):
    bundle_path = _make_evidence_bundle(tmp_path, quick_check=True)
    model = build_report_model_from_bundle(json.loads(bundle_path.read_text(encoding="utf-8")))
    formal = render_markdown(build_formal_calibration_report(model))

    assert model["decision"]["decision_status"] == "candidate_coefficients_generated_no_write"
    assert model["report_release_decision"]["release_status"] == "draft_only"
    assert model["run_evidence_status"]["available"] is True
    assert model["run_evidence_status"]["physical_boundaries"]["opens_com_ports"] is False
    assert any(row["role"] == "run_evidence_status" for row in model["artifact_manifest"])
    assert "封路控压多压力点未用于正式 CO2/H2O 拟合" in formal
    assert "未写入设备" in formal
    assert "开放流通" in formal
    assert "运行证据状态" in formal
    assert "物理边界" in formal
    assert "打开 COM=否" in formal
    assert "控制气路/水路=否" in formal
    assert "非 real acceptance 证据=是" in formal
    assert "DRAFT / NOT FOR FORMAL ISSUE" in formal
    assert model["result_rows"]
    assert {row["component"] for row in model["result_rows"]} == {"CO2", "H2O"}
    assert any(row["source"] == "pressure_channel_bias" for row in model["uncertainty_budget"])


def test_write_v1_5_calibration_reports_outputs_markdown_docx_pdf_and_model(tmp_path):
    bundle_path = _make_evidence_bundle(tmp_path, quick_check=True)
    outputs = write_v1_5_calibration_reports(
        evidence_bundle_path=bundle_path,
        output_dir=tmp_path / "reports",
        report_no="RPT-001",
        reviewer="reviewer-a",
        approver="approver-a",
        location="lab-a",
        calibration_date="2026-05-24",
    )

    expected_keys = {
        "report_model",
        "run_report_markdown",
        "run_report_docx",
        "run_report_pdf",
        "technical_report_markdown",
        "technical_report_docx",
        "technical_report_pdf",
        "formal_calibration_report_markdown",
        "formal_calibration_report_docx",
        "formal_calibration_report_pdf",
        "device_001_calibration_certificate_markdown",
        "device_001_calibration_certificate_docx",
        "device_001_calibration_certificate_pdf",
        "device_001_verification_certificate_markdown",
        "device_001_verification_certificate_docx",
        "device_001_verification_certificate_pdf",
        "per_device_certificate_manifest",
        "per_device_certificate_artifact_hashes",
    }
    assert expected_keys.issubset(set(outputs))
    for key in expected_keys:
        assert outputs[key].exists()
        assert outputs[key].stat().st_size > 0
    assert outputs["formal_calibration_report_pdf"].read_bytes().startswith(b"%PDF-")
    with zipfile.ZipFile(outputs["formal_calibration_report_docx"]) as archive:
        assert "word/document.xml" in archive.namelist()
    report_model = json.loads(outputs["report_model"].read_text(encoding="utf-8"))
    assert report_model["report_no"] == "RPT-001"
    assert report_model["report_release_decision"]["release_status"] == "draft_only"
    certificate_manifest = json.loads(outputs["per_device_certificate_manifest"].read_text(encoding="utf-8"))
    assert certificate_manifest["physical_boundaries"]["opens_com_ports"] is False
    assert certificate_manifest["physical_boundaries"]["writes_coefficients"] is False
    assert any(
        row["artifact_key"] == "device_001_calibration_certificate_docx"
        and row["sha256"]
        and row["certificate_kind"] == "calibration"
        for row in certificate_manifest["artifacts"]
    )
    with outputs["per_device_certificate_artifact_hashes"].open(encoding="utf-8-sig", newline="") as handle:
        hash_rows = list(csv.DictReader(handle))
    assert any(row["artifact_key"] == "device_001_verification_certificate_pdf" for row in hash_rows)
    markdown_outputs = (
        outputs["run_report_markdown"],
        outputs["technical_report_markdown"],
        outputs["formal_calibration_report_markdown"],
    )
    for path in markdown_outputs:
        assert path.read_bytes().startswith(b"\xef\xbb\xbf")
        text = path.read_text(encoding="utf-8-sig")
        _assert_no_mojibake(text)
        assert "运行证据状态" in text
        assert "V1.5 气体分析仪" in text
    assert "物理边界" in outputs["technical_report_markdown"].read_text(encoding="utf-8")


def test_reports_include_per_device_h2o_candidate_rollup_without_warning_blockers(tmp_path):
    bundle_path = _make_evidence_bundle(tmp_path, quick_check=True)
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    bundle["tables"]["coefficient_candidates"] = [
        {
            "component": "h2o",
            "candidate_status": "ready_for_reviewer",
            "allowed_for_review": True,
            "auto_write_allowed": False,
            "blockers": [],
            "metadata": {"analyzer_device_id": "022"},
        },
        {
            "component": "h2o",
            "candidate_status": "candidate_fit_review_required",
            "allowed_for_review": True,
            "auto_write_allowed": False,
            "blockers": [],
            "metadata": {
                "analyzer_device_id": "022",
                "warning_reasons_list": [
                    "side_channel_cache_age_warning_kept_as_evidence_not_fit_blocker"
                ],
            },
        },
        {
            "component": "h2o",
            "candidate_status": "blocked",
            "allowed_for_review": False,
            "auto_write_allowed": False,
            "blockers": ["manual_device_block:firmware_upgrade_required"],
            "metadata": {"analyzer_device_id": "100"},
        },
    ]

    model = build_report_model_from_bundle(bundle)
    h2o_rollup = {
        row["analyzer_device_id"]: row
        for row in model["h2o_candidate_review_rollup"]
    }
    technical = render_markdown(build_technical_report(model))
    formal = render_markdown(build_formal_calibration_report(model))

    assert h2o_rollup["022"]["consolidated_status"] == "review_required"
    assert h2o_rollup["022"]["blockers"] == ""
    assert "side_channel_cache_age_warning_kept_as_evidence_not_fit_blocker" in h2o_rollup["022"]["warnings"]
    assert h2o_rollup["100"]["consolidated_status"] == "blocked"
    assert "H2O 候选设备归并" in technical
    assert "候选系数审核摘要" in formal
    assert "水路候选不得被 CO2 成对写入门禁误判" in technical


def test_report_cli_generates_reports(tmp_path):
    bundle_path = _make_evidence_bundle(tmp_path, quick_check=True)
    output_dir = tmp_path / "cli_reports"

    rc = report_main(
        [
            "--evidence-bundle-json",
            str(bundle_path),
            "--output-dir",
            str(output_dir),
            "--report-no",
            "RPT-CLI",
            "--reviewer",
            "reviewer-a",
            "--approver",
            "approver-a",
        ]
    )

    assert rc == 0
    assert (output_dir / "formal_calibration_report.md").exists()
    assert (output_dir / "formal_calibration_report.docx").exists()
    assert (output_dir / "formal_calibration_report.pdf").exists()


def test_reports_include_post_write_reverification_evidence(tmp_path):
    bundle_path = _make_evidence_bundle(tmp_path, quick_check=True, post_write=True)
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    model = build_report_model_from_bundle(bundle)
    run_markdown = render_markdown(build_run_report(model))
    technical_markdown = render_markdown(build_technical_report(model))
    formal_markdown = render_markdown(build_formal_calibration_report(model))

    assert model["post_write_reverification"]["available"] is True
    assert model["post_write_reverification"]["overall_status"] == "pass"
    assert model["post_write_reverification"]["device_summary"][0]["device_id"] == "100"
    assert "post_write_reverification_passed_or_no_write" in {
        row["check"] for row in model["release_checklist"]
    }
    assert "写后复验" in run_markdown
    assert "写后复验设备汇总" in run_markdown
    assert "写后复验点位明细" in technical_markdown
    assert "写后复验状态：pass" in formal_markdown
    assert "运行证据状态" in run_markdown
    assert "运行证据状态" in technical_markdown
    assert "运行证据状态" in formal_markdown


def test_reports_include_getco_snapshots_write_events_and_point_errors(tmp_path):
    bundle_path = _make_evidence_bundle(tmp_path, quick_check=True, post_write=True)
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    bundle["tables"]["coefficient_snapshots"] = [
        {
            "analyzer_id": "100",
            "snapshot_type": "old_component_getco_coefficients",
            "coefficients": {
                "GETCO1_before": [1, 2, 3, 4, 5, 6],
                "GETCO3_before": [7, 8, 9, 10, 11, 12],
                "GETCO5_before": [-20.0, 1.003],
            },
            "coefficients_hash": "hash-100",
            "metadata": {
                "snapshot_file_name": "old_getco_100.json",
                "getco_groups": ["GETCO1", "GETCO3", "GETCO5"],
            },
        }
    ]
    bundle["tables"]["coefficient_write_events"] = [
        {
            "analyzer_id": "100",
            "event_type": "controlled_senco_write",
            "status": "written_readback_verified",
            "candidate_id": "candidate-100",
            "old_coefficients_hash": "hash-100",
            "approved_by": "reviewer-a",
            "command_summary": "SENCO1/SENCO3/SENCO5 controlled write",
            "readback": {"GETCO1": [1, 2, 3, 4, 5, 6], "GETCO5": [-20.0, 1.003]},
        }
    ]

    model = build_report_model_from_bundle(bundle)
    technical_markdown = render_markdown(build_technical_report(model))
    formal_markdown = render_markdown(build_formal_calibration_report(model))

    assert model["coefficient_snapshot_rows"][0]["GETCO1"] == "[1,2,3,4,5,6]"
    assert model["coefficient_snapshot_rows"][0]["GETCO5"] == "[-20.0,1.003]"
    assert model["coefficient_write_event_rows"][0]["readback"] == (
        '{"GETCO1":[1,2,3,4,5,6],"GETCO5":[-20.0,1.003]}'
    )
    assert "系数证据链" in technical_markdown
    assert "设备系数证据链" in formal_markdown
    assert "GETCO1-9 快照" in formal_markdown
    assert "SENCO1/SENCO3/SENCO5 controlled write" in technical_markdown
    assert "写后复验点位误差" in formal_markdown
    assert "post_write_900ppm" in formal_markdown


def test_blocked_evidence_report_cannot_pose_as_formal_certificate(tmp_path):
    bundle_path = _make_evidence_bundle(tmp_path, quick_check=False)
    outputs = write_v1_5_calibration_reports(
        evidence_bundle_path=bundle_path,
        output_dir=tmp_path / "blocked_reports",
    )
    formal = outputs["formal_calibration_report_markdown"].read_text(encoding="utf-8")
    model = json.loads(outputs["report_model"].read_text(encoding="utf-8"))

    assert model["decision"]["decision_status"] == "blocked"
    assert "证据包当前为 blocked" in formal
    assert "不得作为正式校准证书签发" in formal


def test_released_uncertainty_without_signatures_is_review_ready(tmp_path):
    bundle_path = _make_evidence_bundle(tmp_path, quick_check=True)
    model = build_report_model_from_bundle(
        json.loads(bundle_path.read_text(encoding="utf-8")),
        uncertainty_payload=_released_uncertainty_payload(),
    )

    assert model["uncertainty_summary"]["status"] == "released"
    assert model["report_release_decision"]["release_status"] == "review_ready"
    assert all(row["expanded_uncertainty_k2"] != "not_released" for row in model["result_rows"])


def test_unreleased_uncertainty_template_keeps_report_draft_only(tmp_path):
    bundle_path = _make_evidence_bundle(tmp_path, quick_check=True)
    uncertainty_path = tmp_path / "released_uncertainty_inputs_template.json"
    uncertainty_path.write_text(
        json.dumps(RELEASED_UNCERTAINTY_INPUTS_TEMPLATE, ensure_ascii=False),
        encoding="utf-8",
    )

    outputs = write_v1_5_calibration_reports(
        evidence_bundle_path=bundle_path,
        output_dir=tmp_path / "draft_reports",
        reviewer="reviewer-a",
        approver="approver-a",
        uncertainty_json=uncertainty_path,
    )
    model = json.loads(outputs["report_model"].read_text(encoding="utf-8"))
    formal = outputs["formal_calibration_report_markdown"].read_text(encoding="utf-8")

    assert model["uncertainty_summary"]["status"] == "not_released"
    assert model["report_release_decision"]["release_status"] == "draft_only"
    assert model["report_release_decision"]["formal_issue_allowed"] is False
    assert any("not_evaluated" in item for item in model["uncertainty_summary"]["missing_required"])
    assert all(row["expanded_uncertainty_k2"] == "not_released" for row in model["result_rows"])
    assert "DRAFT / NOT FOR FORMAL ISSUE" in formal


def test_released_uncertainty_with_signatures_is_formal_release_ready(tmp_path):
    bundle_path = _make_evidence_bundle(tmp_path, quick_check=True)
    uncertainty_path = _write_uncertainty_json(tmp_path)
    outputs = write_v1_5_calibration_reports(
        evidence_bundle_path=bundle_path,
        output_dir=tmp_path / "release_ready_reports",
        reviewer="reviewer-a",
        approver="approver-a",
        uncertainty_json=uncertainty_path,
    )
    model = json.loads(outputs["report_model"].read_text(encoding="utf-8"))
    formal = outputs["formal_calibration_report_markdown"].read_text(encoding="utf-8")

    assert model["report_release_decision"]["release_status"] == "formal_release_ready"
    assert model["report_release_decision"]["formal_issue_allowed"] is True
    assert "DRAFT / NOT FOR FORMAL ISSUE" not in formal


def test_unreviewed_write_event_is_not_releasable(tmp_path):
    bundle_path = _make_evidence_bundle(tmp_path, quick_check=True)
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    bundle["tables"]["coefficient_write_events"][0]["status"] = "sent"

    model = build_report_model_from_bundle(
        bundle,
        reviewer="reviewer-a",
        approver="approver-a",
        uncertainty_payload=_released_uncertainty_payload(),
    )

    assert model["report_release_decision"]["release_status"] == "not_releasable"
    assert "coefficient_write_event_requires_audit" in ";".join(model["report_release_decision"]["reasons"])


def test_reports_prepare_per_device_certificate_evidence_from_open_flow_qc(tmp_path):
    bundle_path = _make_evidence_bundle(tmp_path, quick_check=True)
    model = build_report_model_from_bundle(json.loads(bundle_path.read_text(encoding="utf-8")))
    readiness = {
        row["analyzer_device_id"]: row
        for row in model["per_device_certificate_readiness"]
    }
    point_evidence = [
        row
        for row in model["per_device_point_evidence"]
        if row["analyzer_device_id"] == "001"
    ]

    assert "001" in readiness
    assert readiness["001"]["point_evidence_count"] == 2
    assert readiness["001"]["calibratable_A_count"] == 2
    assert readiness["001"]["calibration_certificate_status"] == "draft_or_blocked"
    assert "report_release_status=draft_only" in readiness["001"]["calibration_certificate_reasons"]
    assert {row["component"] for row in point_evidence} == {"CO2", "H2O"}
    assert {row["calibratability_grade"] for row in point_evidence} == {"A"}
    assert all(row["candidate_fit_allowed"] is True for row in point_evidence)


def test_reports_do_not_issue_per_device_certificate_for_aggregate_device_id(tmp_path):
    bundle_path = _make_evidence_bundle(tmp_path, quick_check=True)
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    bundle["tables"].setdefault("coefficient_snapshots", []).extend(
        [
            {
                "analyzer_id": "077",
                "snapshot_type": "old_component_getco_coefficients",
                "coefficients": {"GETCO1_before": [1, 2, 3]},
                "coefficients_hash": "hash-077",
            },
            {
                "analyzer_id": "001;077",
                "snapshot_type": "run_level_aggregate_snapshot",
                "coefficients": {"GETCO1_before": [4, 5, 6]},
                "coefficients_hash": "hash-aggregate",
            },
        ]
    )
    bundle["tables"].setdefault("coefficient_write_events", []).append(
        {
            "analyzer_id": "001;077",
            "event_type": "aggregate_review_marker",
            "status": "not_a_device_write",
        }
    )
    aggregate_bundle_path = tmp_path / "aggregate_guard_evidence_bundle.json"
    aggregate_bundle_path.write_text(
        json.dumps(bundle, ensure_ascii=False),
        encoding="utf-8",
    )

    outputs = write_v1_5_calibration_reports(
        evidence_bundle_path=aggregate_bundle_path,
        output_dir=tmp_path / "aggregate_guard_reports",
    )
    model = json.loads(outputs["report_model"].read_text(encoding="utf-8"))
    readiness_ids = {
        row["analyzer_device_id"]
        for row in model["per_device_certificate_readiness"]
    }

    assert "001" in readiness_ids
    assert "077" in readiness_ids
    assert "001;077" not in readiness_ids
    assert not any("001_077" in key for key in outputs)


def test_reports_apply_h2o_queue_exclusion_as_diagnostic_only_fit_blocker(tmp_path):
    bundle_path = _make_evidence_bundle(tmp_path, quick_check=True)
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    open_flow_file = next(
        row
        for row in bundle["tables"]["sample_files"]
        if row["path"].endswith("open_flow_run_summary.csv")
    )
    open_flow_path = Path(open_flow_file["path"])
    with open_flow_path.open(encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    for row in rows:
        if row.get("component") == "h2o":
            row["point_id"] = "h2o_abort_point"
            row["candidate_fit_allowed"] = "True"
            break
    _write_csv(open_flow_path, rows)

    exclusion_path = tmp_path / "queue_abort_exclusion.csv"
    _write_csv(
        exclusion_path,
        [
            {
                "point_id": "h2o_abort_point",
                "component": "h2o",
                "exclude_from_fit": "true",
                "exclude_from_acceptance": "true",
                "exclude_from_senco_review": "true",
                "exclusion_reason": "operator_aborted_h2o_point",
                "physical_meaning": "aborted H2O point is diagnostic evidence only",
            }
        ],
    )
    bundle["tables"]["sample_files"].append(
        {
            "artifact_role": "h2o_queue_exclusion",
            "path": str(exclusion_path),
            "sha256": "test-hash",
            "required": False,
        }
    )

    model = build_report_model_from_bundle(bundle)
    h2o_evidence = next(
        row for row in model["per_device_point_evidence"] if row["component"] == "H2O"
    )

    assert model["h2o_queue_exclusions"]
    assert h2o_evidence["calibratability_grade"] == "C"
    assert h2o_evidence["fit_input_role"] == "diagnostic_only_queue_exclusion"
    assert h2o_evidence["candidate_fit_allowed"] is False
    assert h2o_evidence["queue_exclusion_status"] == "excluded"
    assert "h2o_queue_abort_exclusion" in h2o_evidence["candidate_fit_blockers"]
